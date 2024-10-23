import pandas as pd # type: ignore
from funcs.utils import jira_extraction, transform_extracted_data, load_extraction

def main():
    output_path: str = 'data/processed/'
    output_name: str = 'extraction_jira.csv'

    extracted_data: pd.DataFrame = jira_extraction()
    transformed_data: pd.DataFrame = transform_extracted_data(extracted_data)
    load_extraction(transformed_data,output_path,output_name)

if __name__ == '__main__':
    main()