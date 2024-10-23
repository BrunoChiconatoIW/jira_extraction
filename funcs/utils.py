import os
import pandas as pd # type: ignore

from jira import JIRA # type: ignore
from dotenv import load_dotenv # type: ignore
from loguru import logger # type: ignore
from typing import Optional
from datetime import datetime

from decorators.decorators import log_decorator, time_decorator

logger.add('./app.log', format="{time:DD/MM/YYYY HH:mm:ss} {level} {message}", level="INFO")

def reformat_date(date_str) -> str:
    """
    Reformata uma string de data do formato ISO 8601 para "dd/mm/aaaa HH:MM:SS".

    Esta função converte uma string de data no formato "%Y-%m-%dT%H:%M:%S.%f%z"
    (ISO 8601) para um formato mais legível, "dd/mm/aaaa HH:MM:SS". Se a entrada
    for 'N/A' ou um valor nulo, a função retorna a entrada sem alterações.

    Parameters:
        date_str (str): A string de data a ser reformatada. Espera-se que esteja
                        no formato "%Y-%m-%dT%H:%M:%S.%f%z" ou 'N/A'.

    Returns:
        str: A data reformatada no formato "dd/mm/aaaa HH:MM:SS" se a entrada for
             uma string de data válida. Retorna a entrada original se for 'N/A'
             ou nula.
    """
    if pd.isnull(date_str) or date_str == 'N/A':
        return date_str
    else:
        date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f%z")
        return date_obj.strftime("%d/%m/%Y %H:%M:%S")

@time_decorator
@log_decorator
def jira_extraction(
    jira_server: Optional[str] = None,
    email: Optional[str] = None,
    api_token: Optional[str] = None,
    board_id: Optional[str] = None,
    project_key: str = 'Gestão de Atividades'
) -> pd.DataFrame:
    """
    Extrai dados de issues do JIRA, incluindo worklogs, e retorna como um DataFrame do pandas.

    Parameters:
        jira_server (str, opcional): A URL do servidor JIRA. Se None, lê da variável de ambiente 'JIRA_SERVER'.
        email (str, opcional): O endereço de email para autenticação no JIRA. Se None, lê da variável 'JIRA_EMAIL'.
        api_token (str, opcional): O token de API para autenticação no JIRA. Se None, lê da variável 'JIRA_API_TOKEN'.
        board_id (str, opcional): O ID do board do JIRA. Se None, lê da variável 'JIRA_BOARD_ID'.
        project_key (str): A chave do projeto JIRA. Padrão é 'Gestão de Atividades'.

    Returns:
        pd.DataFrame: Um DataFrame contendo os dados das issues e dos worklogs.

    Raises:
        ValueError: Se as informações de autenticação necessárias estiverem faltando.
        Exception: Para quaisquer outras exceções que ocorram durante a extração de dados.
    """
    try:
        if jira_server is None or email is None or api_token is None or board_id is None:
            load_dotenv()
            jira_server = jira_server or os.getenv('JIRA_SERVER')
            email = email or os.getenv('JIRA_EMAIL')
            api_token = api_token or os.getenv('JIRA_API_TOKEN')
            board_id = board_id or os.getenv('JIRA_BOARD_ID')

        if not jira_server or not email or not api_token or not board_id:
            raise ValueError("Servidor, e-mail, token da API e ID do board do JIRA devem ser fornecidos!")

        jira = JIRA(server=jira_server, basic_auth=(email, api_token))
        logger.info('Conexão ao servidor do JIRA bem sucedida.')

        sprints = jira.sprints(board_id, state="active,closed")
        sprint_map = {sprint.id: sprint.name for sprint in sprints}
        logger.info(f"Foram encontradas '{len(sprints)}' sprints associadas ao board de ID '{board_id}'.")

        combined_data = []

        logger.info('Processando issues com sprints associadas.')
        for sprint_id, sprint_name in sprint_map.items():
            jql_query = f"project = 'Gestão de Atividades' AND sprint = {sprint_id} ORDER BY created DESC"
            start_at = 0
            max_results = 100
            

            while True:
                issues = jira.search_issues(jql_query, startAt=start_at, maxResults=max_results)
                logger.info(f"Encontrado {len(issues)} issues começando em {start_at}.")
                for issue in issues:
                    issue_info = {
                        'Issue Key': issue.key,
                        'Summary': issue.fields.summary,
                        'Priority': getattr(issue.fields.priority, 'name', 'No Priority'),
                        'Labels': issue.fields.labels,
                        'Assignee': issue.fields.assignee.displayName if issue.fields.assignee else 'Unassigned',
                        'Status': issue.fields.status.name,
                        'Creator': issue.fields.creator.displayName,
                        'Custom Field 10160': getattr(issue.fields, 'customfield_10160', 'N/A'),
                        'Custom Field 10163': getattr(issue.fields, 'customfield_10163', 'N/A'),
                        'Custom Field 10175': getattr(issue.fields, 'customfield_10175', 'N/A'),
                        'Time Spent': issue.fields.timespent if issue.fields.timespent else 'No Time Spent',
                        'Project': issue.fields.project.name,
                        'Created': issue.fields.created,
                        'Updated': issue.fields.updated,
                        'Description': issue.fields.description if issue.fields.description else 'No Description',
                        'Sprint': sprint_name,
                        'E-mail Solicitante': getattr(issue.fields, 'customfield_10090', 'N/A'),
                        'Nome do Solicitante': getattr(issue.fields, 'customfield_10089', 'N/A'),
                        'Start Date': getattr(issue.fields, 'customfield_10015', 'N/A'),
                        'End Data': getattr(issue.fields, 'customfield_10152', 'N/A'),
                        'Parent': getattr(issue.fields, 'parent', 'N/A')
                    }

                    worklogs = issue.fields.worklog.worklogs
                    if worklogs:
                        for worklog in worklogs:
                            combined_data.append({
                                **issue_info,
                                'Worklog Author': getattr(worklog.author, 'displayName', 'No Author'),
                                'Worklog Comment': getattr(worklog, 'comment', 'No Comment'),
                                'Worklog Created': getattr(worklog, 'created', 'N/A'),
                                'Worklog Started': getattr(worklog, 'started', 'N/A'),
                                'Worklog Time Spent': getattr(worklog, 'timeSpent', 'N/A'),
                                'Worklog Time Spent Seconds': getattr(worklog, 'timeSpentSeconds', 0)
                            })
                    else:
                        combined_data.append({
                            **issue_info,
                            'Worklog Author': 'No Worklog',
                            'Worklog Comment': 'No Worklog',
                            'Worklog Created': 'N/A',
                            'Worklog Started': 'N/A',
                            'Worklog Time Spent': 'N/A',
                            'Worklog Time Spent Seconds': 0
                        })
                        
                start_at += max_results
                if len(issues) < max_results:
                    break

        logger.info('Processamento finalizado.')

        jql_query = "project = 'Gestão de Atividades' AND sprint is EMPTY ORDER BY created DESC"
        start_at = 0

        logger.info('Processando issues sem sprints associadas.')
        while True:
            issues = jira.search_issues(jql_query, startAt=start_at, maxResults=max_results)
            logger.info(f"Encontrado {len(issues)} issues começando em {start_at}.")
            for issue in issues:
                issue_info = {
                    'Issue Key': issue.key,
                    'Summary': issue.fields.summary,
                    'Priority': getattr(issue.fields.priority, 'name', 'No Priority'),
                    'Labels': issue.fields.labels,
                    'Assignee': issue.fields.assignee.displayName if issue.fields.assignee else 'Unassigned',
                    'Status': issue.fields.status.name,
                    'Creator': issue.fields.creator.displayName,
                    'Custom Field 10160': getattr(issue.fields, 'customfield_10160', 'N/A'),
                    'Custom Field 10163': getattr(issue.fields, 'customfield_10163', 'N/A'),
                    'Custom Field 10175': getattr(issue.fields, 'customfield_10175', 'N/A'),
                    'Time Spent': issue.fields.timespent if issue.fields.timespent else 'No Time Spent',
                    'Project': issue.fields.project.name,
                    'Created': issue.fields.created,
                    'Updated': issue.fields.updated,
                    'Description': issue.fields.description if issue.fields.description else 'No Description',
                    'Sprint': 'No Sprint',
                    'E-mail Solicitante': getattr(issue.fields, 'customfield_10090', 'N/A'),
                    'Nome do Solicitante': getattr(issue.fields, 'customfield_10089', 'N/A'),
                    'Start Date': getattr(issue.fields, 'customfield_10015', 'N/A'),
                    'End Data': getattr(issue.fields, 'customfield_10152', 'N/A'),
                    'Parent': getattr(issue.fields, 'parent', 'N/A')
                }

                worklogs = issue.fields.worklog.worklogs
                if worklogs:
                    for worklog in worklogs:
                        combined_data.append({
                            **issue_info,
                            'Worklog Author': getattr(worklog.author, 'displayName', 'No Author'),
                            'Worklog Comment': getattr(worklog, 'comment', 'No Comment'),
                            'Worklog Created': getattr(worklog, 'created', 'N/A'),
                            'Worklog Started': getattr(worklog, 'started', 'N/A'),
                            'Worklog Time Spent': getattr(worklog, 'timeSpent', 'N/A'),
                            'Worklog Time Spent Seconds': getattr(worklog, 'timeSpentSeconds', 0)
                        })
                else:
                    combined_data.append({
                        **issue_info,
                        'Worklog Author': 'No Worklog',
                        'Worklog Comment': 'No Worklog',
                        'Worklog Created': 'N/A',
                        'Worklog Started': 'N/A',
                        'Worklog Time Spent': 'N/A',
                        'Worklog Time Spent Seconds': 0
                    })

            start_at += max_results
            if len(issues) < max_results:
                break

        logger.info('Processamento finalizado.')

        df_combined = pd.DataFrame(combined_data)
        logger.info('Extração dos dados do JIRA concluída.')
        return df_combined

    except Exception as e:
        logger.error(f"Um erro ocorreu ao extrair os dados do JIRA: {e}")
        raise

@time_decorator
@log_decorator
def transform_extracted_data(extracted_data: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma os dados extraídos, ajustando as colunas de data.

    Esta função aplica a função `reformat_date` às colunas de data especificadas
    no DataFrame, convertendo as datas para um formato mais legível.

    Parameters:
        extracted_data (pd.DataFrame): DataFrame contendo os dados extraídos que
                                       incluem colunas de datas a serem reformadas.

    Returns:
        pd.DataFrame: DataFrame com as colunas de datas transformadas.

    Raises:
        ValueError: Se qualquer coluna de data não existir no DataFrame.
        Exception: Para quaisquer outros erros que possam ocorrer durante a transformação.
    """
    try:
        date_columns = ['Worklog Created', 'Worklog Started']
        
        for col in date_columns:
            if col not in extracted_data.columns:
                raise ValueError(f"A coluna '{col}' não foi encontrada no DataFrame.")

        for col in date_columns:
            extracted_data[col] = extracted_data[col].apply(reformat_date)
        
        return extracted_data

    except ValueError as ve:
        logger.error(f"Erro de valor: {ve}")
        raise

    except Exception as e:
        logger.error(f"Erro ao transformar os dados extraídos: {e}")
        raise

@time_decorator
@log_decorator
def load_extraction(transformed_data: pd.DataFrame, output_path: str, output_name: str) -> None:
    """
    Salva os dados transformados em um arquivo CSV.

    Esta função recebe um DataFrame com os dados transformados e os salva em um 
    arquivo CSV no caminho especificado. O arquivo será salvo sem o índice do DataFrame.

    Parameters:
        transformed_data (pd.DataFrame): O DataFrame contendo os dados transformados 
                                         que serão exportados para um arquivo CSV.
        output_path (str): O caminho do diretório onde o arquivo CSV será salvo.
        output_name (str): O nome do arquivo CSV a ser criado.

    Returns:
        None: Esta função não retorna nenhum valor.

    Raises:
        FileNotFoundError: Se o caminho especificado não puder ser encontrado.
        PermissionError: Se não houver permissão para criar o diretório ou salvar o arquivo.
        OSError: Se ocorrer um erro no sistema operacional ao criar diretórios ou salvar o arquivo.
        ValueError: Se o DataFrame tiver problemas que impeçam a exportação.
    """
    try:
        if not os.path.exists(output_path):
            os.makedirs(output_path, exist_ok=True)

        output = os.path.join(output_path, output_name)

        transformed_data.to_csv(output, index=False)
        logger.info(f"Arquivo salvo com sucesso em: {output}")

    except FileNotFoundError as fnf_error:
        logger.error(f"Erro: Caminho não encontrado - {fnf_error}")
        raise

    except PermissionError as perm_error:
        logger.error(f"Erro: Permissão negada - {perm_error}")
        raise

    except OSError as os_error:
        logger.error(f"Erro do sistema operacional - {os_error}")
        raise

    except ValueError as val_error:
        logger.error(f"Erro ao salvar o DataFrame - {val_error}")
        raise

    except Exception as e:
        logger.error(f"Ocorreu um erro inesperado: {e}")
        raise