import json
import boto3
import os
from botocore.exceptions import ClientError
import time
from openai import OpenAI
import logging

bucket = 'https://textract-console-us-east-2-8a84b6c0-d3c2-420d-8d03-9d742797b315.s3.us-east-2.amazonaws.com/'
document = '5014e3c4_f3de_4190_ba69_6528f4a0f5eb_benjamin_moore_advance_matte.pdf'
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler_full(event, context):
    # Initialize AWS clients
    textract = boto3.client('textract')
    s3 = boto3.client('s3')

    event = {
        'bucket':'textract-console-us-east-2-8a84b6c0-d3c2-420d-8d03-9d742797b315',
        "document":'5014e3c4_f3de_4190_ba69_6528f4a0f5eb_benjamin_moore_advance_matte.pdf'
    }
    
    try:
        # Validate input
        if 'bucket' not in event or 'document' not in event:
            return {
                'statusCode': 400,
                'body': json.dumps('Missing required parameters: bucket and document')
            }
            
        bucket = event['bucket']
        document = event['document']
        
        # Validate document exists in S3
        try:
            s3.head_object(Bucket=bucket, Key=document)
        except ClientError as e:
            return {
                'statusCode': 404,
                'body': json.dumps(f'Document not found in S3: {str(e)}')
            }
        
        # Define custom queries
        queries = [
            {"Text": "What is the CAS number for this product?"},
            {"Text": "What is the CAS no. listed?"},
            {"Text": "What are the ingredient names in this product?"},
            {"Text": "What is the chemical name shown?"},
            {"Text": "What is the chemical composition of this product?"},
            {"Text": "List the CAS number"},
            {"Text": "Show me the ingredients"},
            {"Text": "What chemicals are in this product?"}
        ]
        
        # Start document analysis
        start_response = textract.start_document_analysis(
            DocumentLocation={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': document
                }
            },
            FeatureTypes=['TABLES', 'QUERIES'],
            QueriesConfig={
                'Queries': queries
            }
        )
        
        job_id = start_response['JobId']
        
        # Poll for job completion
        print(f"Starting to poll for results. Job ID: {job_id}")
        max_retries = 40  # Maximum number of retries (10 minutes with 15-second intervals)
        retries = 0
        
        while retries < max_retries:
            response = textract.get_document_analysis(JobId=job_id)
            status = response['JobStatus']
            print(f"Current status: {status}")
            
            if status == 'SUCCEEDED':
                break
            elif status == 'FAILED':
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'error': 'Document analysis failed',
                        'details': response.get('StatusMessage', 'Unknown error')
                    })
                }
            
            time.sleep(15)  # Wait 15 seconds before polling again
            retries += 1
        
        if retries >= max_retries:
            return {
                'statusCode': 504,
                'body': json.dumps('Timeout waiting for analysis to complete')
            }
        
        # Initialize results dictionary with all queries
        query_results = {query['Text']: {
            'query': query['Text'],
            'answer': 'No match found',
            'confidence': 0
        } for query in queries}
        
        # Process all pages of results
        while True:
            for block in response['Blocks']:
                if block['BlockType'] == 'QUERY_RESULT':
                    print(f"Processing query result block: {json.dumps(block, default=str)}")
                    
                    answer_text = block.get('Text', '')
                    confidence = block.get('Confidence', 0)
                    
                    # Get the query text from the block
                    if 'Query' in block and isinstance(block['Query'], dict):
                        query_text = block['Query'].get('Text', '')
                        if query_text in query_results:
                            query_results[query_text] = {
                                'query': query_text,
                                'answer': answer_text,
                                'confidence': confidence
                            }
            
            if 'NextToken' in response:
                response = textract.get_document_analysis(
                    JobId=job_id,
                    NextToken=response['NextToken']
                )
            else:
                break
        
         # Process tables
        tables = []
        all_blocks = []
        
        # Reset response to get all blocks again
        response = textract.get_document_analysis(JobId=job_id)
        while True:
            all_blocks.extend(response['Blocks'])
            if 'NextToken' in response:
                response = textract.get_document_analysis(
                    JobId=job_id,
                    NextToken=response['NextToken']
                )
            else:
                break
        
        # Process tables
        blocks_map = {block['Id']: block for block in all_blocks}
        table_blocks = [block for block in all_blocks if block['BlockType'] == 'TABLE']
        
        for table_block in table_blocks:
            table = extract_table_data(table_block, blocks_map)
            if table:
                tables.append(table)
        
        result = {
            'tables': tables,
            'query_results': list(query_results.values()),  # Convert dict to list
            'debug_info': {
                'total_blocks': len(all_blocks),
                'table_blocks_found': len(table_blocks),
                'submitted_queries': queries
            }
        }
        
        print(f"Final result: {json.dumps(result, indent=2, default=str)}")
        
        return json.dumps(result)
            
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Unexpected error: {str(e)}',
                'traceback': traceback.format_exc()
            })
        }

def extract_table_data(table_block, blocks_map):
    """Extract data from table block"""
    if 'Relationships' not in table_block:
        return None

    # Get all cells for this table
    cells = []
    for relationship in table_block['Relationships']:
        if relationship['Type'] == 'CHILD':
            for cell_id in relationship['Ids']:
                if cell_id in blocks_map:
                    cell = blocks_map[cell_id]
                    if cell['BlockType'] == 'CELL':
                        cells.append(cell)
    
    if not cells:
        return None

    # Find table dimensions
    max_row = max(cell['RowIndex'] for cell in cells)
    max_col = max(cell['ColumnIndex'] for cell in cells)
    
    # Initialize table
    table = [['' for _ in range(max_col)] for _ in range(max_row)]
    
    # Fill table with cell contents
    for cell in cells:
        row_idx = cell['RowIndex'] - 1
        col_idx = cell['ColumnIndex'] - 1
        
        # Get cell content
        content = ''
        if 'Relationships' in cell:
            for relationship in cell['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for word_id in relationship['Ids']:
                        if word_id in blocks_map:
                            word_block = blocks_map[word_id]
                            if word_block['BlockType'] in ['WORD', 'NUMBER']:
                                content += word_block['Text'] + ' '
        else:
            content = cell.get('Text', '')
        
        table[row_idx][col_idx] = content.strip()
    
    # Remove empty rows
    table = [row for row in table if any(cell != '' for cell in row)]
    
    return table if table else None


def lambda_handler(event, context):

    extracted_information = {"tables": [[["Product Name", "ADVANCE MATTE FINISH BASE 3"], ["Product Code", "7913X"], ["Alternate Product Code", "7913X"], ["Product Class", "Water thinned paint"], ["Color", "All"], ["Recommended use", "Paint"], ["Restrictions on use", "No information available"]], [["Chemical name", "CAS No.", "Weight-%"], ["Nepheline syenite", "37244-96-5", "10 15"], ["Limestone", "1317-65-3", "5 10"], ["Kaolin, calcined", "92704-41-1", "5 10"], ["Titanium dioxide", "13463-67-7", "1 5"], ["Silicon dioxide, wax coated", "112926-00-8", "1 5"], ["Talc", "14807-96-6", "1 5"], ["Polyalkylene glycol alkyl ether", "-", "1 5"], ["Propylene glycol", "57-55-6", "1 - 5"]], [["General Advice", "No hazards which require special first aid measures."], ["Eye Contact", "Rinse thoroughly with plenty of water for at least 15 minutes and consult a physician."], ["Skin Contact", "Wash off immediately with soap and plenty of water while removing all contaminated clothes and shoes."], ["Inhalation", "Move to fresh air. If symptoms persist, call a physician."], ["Ingestion", "Clean mouth with water and afterwards drink plenty of water. Consult a physician if necessary."], ["Most Important Symptoms/Effects", "None known."], ["Notes To Physician", "Treat symptomatically."]], [["5. FIRE-FIGHTING", "MEASURES"], ["Suitable Extinguishing Media", "Use extinguishing measures that are appropriate to local circumstances and the surrounding environment."], ["Protective equipment and precautions for firefighters", "As in any fire, wear self-contained breathing apparatus pressure-demand, MSHA/NIOSH (approved or equivalent) and full protective gear."], ["Specific Hazards Arising From The Chemical", "Closed containers may rupture if exposed to fire or extreme heat."], ["Sensitivity to mechanical impact", "No"], ["Sensitivity to static discharge", "No"], ["Flash Point Data", ""], ["Flash point (F)", "Not applicable"], ["Flash Point (C)", "Not applicable"], ["Method", "Not applicable"]], [["Personal Precautions", "Avoid contact with skin, eyes and clothing. Ensure adequate ventilation."], ["Other Information", "Prevent further leakage or spillage if safe to do SO."], ["Environmental precautions", "See Section 12 for additional Ecological Information."], ["Methods for Cleaning Up", "Soak up with inert absorbent material. Sweep up and shovel into suitable containers for disposal."]], [["", "7. HANDLING AND STORAGE"], ["Handling", "Avoid contact with skin, eyes and clothing. Avoid breathing vapors, spray mists or sanding dust. In case of insufficient ventilation, wear suitable respiratory equipment."], ["Storage", "Keep container tightly closed. Keep out of the reach of children."], ["Incompatible Materials", "No information available"]], [["Chemical name", "ACGIH TLV", "OSHA PEL"], ["Limestone", "N/E", "15 mg/m\u00b3 TWA 5 mg/m\u00b3 TWA"], ["Titanium dioxide", "10 mg/m\u00b3 TWA", "15 mg/m\u00b3 TWA"], ["Silicon dioxide, wax coated", "N/E", "20 mppcf TWA -"], ["Talc", "2 mg/m\u00b3 TWA", "20 mppcf TWA"]], [["Eye/Face Protection", "Safety glasses with side-shields."], ["Skin Protection", "Protective gloves and impervious clothing."], ["Respiratory Protection", "In case of insufficient ventilation wear suitable respiratory equipment."], ["Hygiene Measures", "Avoid contact with skin, eyes and clothing. Remove and wash contaminated clothing before re-use. Wash thoroughly after handling."]], [["Appearance", "liquid"], ["Odor", "little or no odor"], ["Odor Threshold", "No information available"], ["Density (lbs/gal)", "10.7 11.1"], ["Specific Gravity", "1.28 1.33"], ["pH", "No information available"], ["Viscosity (cps)", "No information available"], ["Solubility(ies)", "No information available"], ["Water solubility", "No information available"], ["Evaporation Rate", "No information available"], ["Vapor pressure", "No information available"], ["Vapor density", "No information available"], ["Wt. % Solids", "50 60"], ["Vol. % Solids", "40 50"], ["Wt. % Volatiles", "40 50"], ["Vol. % Volatiles", "50 60"], ["VOC Regulatory Limit (g/L)", "< 50"], ["Boiling Point (\u00b0F)", "212"], ["Boiling Point (C)", "100"], ["Freezing point (\u00b0F)", "32"], ["Freezing Point (C)", "0"], ["Flash point (\u00b0F)", "Not applicable"], ["Flash Point (C)", "Not applicable"], ["Method", "Not applicable"], ["Flammability (solid, gas)", "Not applicable"], ["Upper flammability limit:", "Not applicable"], ["Lower flammability limit:", "Not applicable"], ["Autoignition Temperature (\u00b0F)", "No information available"], ["Autoignition Temperature (C)", "No information available"], ["Decomposition Temperature (\u00b0F)", "No information available"], ["Decomposition Temperature (C)", "No information available"], ["Partition coefficient", "No information available"]], [["Conditions to avoid", "Prevent from freezing."], ["Incompatible Materials", "No materials to be especially mentioned."], ["Hazardous Decomposition Products", "None under normal use."], ["Possibility of hazardous reactions", "None under normal conditions of use."]], [["Eye contact", "May cause slight irritation."], ["Skin contact", "Substance may cause slight skin irritation. Prolonged or repeated contact may dry skin and cause irritation."], ["Inhalation", "Inhalation of vapors in high concentration may cause irritation of respiratory system. Avoid breathing vapors or mists."], ["Ingestion", "Ingestion may cause gastrointestinal irritation, nausea, vomiting and diarrhea."], ["Sensitization", "No information available"], ["Neurological Effects", "No information available."], ["Mutagenic Effects", "No information available."], ["Reproductive Effects", "No information available."], ["Developmental Effects", "No information available."], ["Target organ effects", "No information available."], ["STOT - single exposure", "No information available."], ["STOT - repeated exposure", "No information available."], ["Other adverse effects", "No information available."], ["Aspiration Hazard", "No information available"]], [["Chemical name", "Oral LD50", "Dermal LD50", "Inhalation LC50"], ["Kaolin, calcined 92704-41-1", "> 2000 mg/kg (Rat)", "-", ""]], [["Titanium dioxide 13463-67-7", "> 10000 mg/kg ( Rat)", "", "-"], ["Propylene glycol 57-55-6", "= 20 g/kg ( Rat)", "= 20800 mg/kg ( Rabbit", "-"]], [["Chemical name", "IARC", "NTP", "OSHA"], ["Titanium dioxide", "2B - Possible Human Carcinogen", "", "Listed"]], [["", "14. TRANSPORT INFORMATION"], ["DOT", "Not regulated"], ["ICAO / IATA", "Not regulated"], ["IMDG / IMO", "Not regulated"]], [["Acute health hazard", "No"], ["Chronic Health Hazard", "No"], ["Fire hazard", "No"], ["Sudden release of pressure hazard", "No"], ["Reactive Hazard", "No"]], [["Chemical name", "Massachusetts", "New Jersey", "Pennsylvania"], ["Limestone", "", "", ""], ["Titanium dioxide", "", "", ""], ["Silicon dioxide, wax coated", "", "", ""], ["Talc", "", "", ""]]], "query_results": [{"query": "What is the CAS number for this product?", "answer": "No match found", "confidence": 0}, {"query": "What is the CAS no. listed?", "answer": "No match found", "confidence": 0}, {"query": "What are the ingredient names in this product?", "answer": "No match found", "confidence": 0}, {"query": "What is the chemical name shown?", "answer": "No match found", "confidence": 0}, {"query": "What is the chemical composition of this product?", "answer": "No match found", "confidence": 0}, {"query": "List the CAS number", "answer": "No match found", "confidence": 0}, {"query": "Show me the ingredients", "answer": "No match found", "confidence": 0}, {"query": "What chemicals are in this product?", "answer": "No match found", "confidence": 0}], "debug_info": {"total_blocks": 2816, "table_blocks_found": 17, "submitted_queries": [{"Text": "What is the CAS number for this product?"}, {"Text": "What is the CAS no. listed?"}, {"Text": "What are the ingredient names in this product?"}, {"Text": "What is the chemical name shown?"}, {"Text": "What is the chemical composition of this product?"}, {"Text": "List the CAS number"}, {"Text": "Show me the ingredients"}, {"Text": "What chemicals are in this product?"}]}}


    results = analyze_results_with_LLM(extracted_information)

    return json.dumps(results)
 
def analyze_results_with_LLM(extracted_information):
    try:
        # Log the input
        logger.info("Starting analysis with input type: %s", type(extracted_information))
        
        # Ensure extracted_information is properly serialized
        if isinstance(extracted_information, str):
            data = json.loads(extracted_information)
        else:
            data = extracted_information
            extracted_information = json.dumps(data, ensure_ascii=False)
        
        logger.info("Input data validated as JSON")

        # Initialize OpenAI client
        try:
            client = OpenAI(
                api_key=os.environ.get('OPENAI_API_KEY')
                )
            logger.info("OpenAI client initialized")
        except Exception as e:
            logger.error("Failed to initialize OpenAI client: %s", str(e))
            raise
 
        # Create a more structured prompt
        messages = [
            {
                "role": "system",
                "content": """You are a precise data extraction assistant in the chemical industry. Extract chemicals and their CAS numbers from the provided data.
                Return only valid JSON matching the specified structure. Include all chemicals found in either tables or query results."""
            },
            {
                "role": "user",
                "content": f"""Analyze this document data and extract all chemicals and their CAS numbers in JSON format, only return the JSON.
                Here is the document data to analyze: {extracted_information}"""
            }
        ]
        # Make the API call to OpenAI
        response = client.chat.completions.create(
            model="gpt-4o-2024-08-06",
            messages=messages,
            response_format={ "type": "json_object" },
            max_tokens=10000,
            temperature=0.1
        )
        # Extract and parse the response
        answer = response.choices[0].message.content
        parsed_answer = json.loads(answer)
        
        # Ensure the response has the correct structure
        if 'chemicals' not in parsed_answer:
            parsed_answer = {'chemicals': []}
        
        return {
            'statusCode': 200,
            'body': json.dumps(parsed_answer)
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'chemicals': []
            })
        }