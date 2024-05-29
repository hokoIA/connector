import requests
import sqlite3
import json

# Configurações
BASE_URL = "https://graph.facebook.com/v20.0"
ACCOUNT_ID = "act_2583115008677521"
ACCESS_TOKEN = "EAAMCthppO5cBO5T1uWgv0RZC2zM2WyoNPvLeR9wawZBfJfJwIpgD3KGeTEAL3hPcGIFUlnWRqNmQBsYYdW31q6cAee9yQ1gBfdPZA3gGnXptgJda24igOgF5691UZAhQjRnvCJbZAvzDvZC3SauMuaOxlWpxszZBES2ulZBKcxhfSXVeJYRnZBHXcOk1elM6KTKV7MBMGQxCcMFDiNcfA"
LEVEL = "ad"
TIME_RANGE = {"since": "2024-01-01", "until": "2024-05-29"}
FIELDS = "ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,created_time,updated_time,impressions,reach,spend,clicks,ctr,cpc,cpm,cpp,frequency,actions,unique_actions,cost_per_action_type,cost_per_inline_link_click,cost_per_inline_post_engagement,cost_per_unique_click,unique_clicks,unique_ctr,unique_inline_link_click_ctr,unique_inline_link_clicks,inline_link_clicks,inline_post_engagement,cost_per_unique_action_type,conversion_rate_ranking,quality_ranking,engagement_rate_ranking"

# Conectar ao banco de dados SQLite
conexao = sqlite3.connect('../banco-de-dados/AgencIA.db')
cursor = conexao.cursor()

# Função para transformar métricas em formato desejado
def transform_metrics(metrics):
    if metrics:
        return json.dumps([item for sublist in metrics for item in sublist.values()])
    return "[]"

# Função para inserir dados no banco de dados
def insert_insights(data):
    insert_query = '''
    INSERT INTO Insights_Meta (
        ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, 
        created_time, updated_time, impressions, reach, spend, clicks, ctr, cpc, 
        cpm, cpp, frequency, actions, unique_actions, cost_per_action_type, 
        cost_per_inline_link_click, cost_per_inline_post_engagement, 
        cost_per_unique_click, unique_clicks, unique_ctr, 
        unique_inline_link_click_ctr, unique_inline_link_clicks, 
        inline_link_clicks, inline_post_engagement, cost_per_unique_action_type, 
        conversion_rate_ranking, quality_ranking, engagement_rate_ranking
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    cursor.execute(insert_query, (
        data.get('ad_id'), data.get('ad_name'), data.get('adset_id'), data.get('adset_name'), 
        data.get('campaign_id'), data.get('campaign_name'), data.get('created_time'), data.get('updated_time'), 
        data.get('impressions'), data.get('reach'), data.get('spend'), data.get('clicks'), data.get('ctr'), 
        data.get('cpc'), data.get('cpm'), data.get('cpp'), data.get('frequency'), 
        transform_metrics(data.get('actions')), transform_metrics(data.get('unique_actions')), 
        transform_metrics(data.get('cost_per_action_type')), data.get('cost_per_inline_link_click'), 
        data.get('cost_per_inline_post_engagement'), data.get('cost_per_unique_click'), 
        data.get('unique_clicks'), data.get('unique_ctr'), data.get('unique_inline_link_click_ctr'), 
        data.get('unique_inline_link_clicks'), data.get('inline_link_clicks'), data.get('inline_post_engagement'), 
        transform_metrics(data.get('cost_per_unique_action_type')), data.get('conversion_rate_ranking'), 
        data.get('quality_ranking'), data.get('engagement_rate_ranking')
    ))
    conexao.commit()

# Fazer a solicitação à API do Facebook
def fetch_insights():
    url = f"{BASE_URL}/{ACCOUNT_ID}/insights"
    params = {
        'level': LEVEL,
        'time_range': json.dumps(TIME_RANGE),  # Convertendo o time_range para JSON
        'fields': FIELDS,
        'access_token': ACCESS_TOKEN
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        insights = response.json().get('data', [])
        for insight in insights:
            insert_insights(insight)
        print("Dados inseridos com sucesso.")
    else:
        print(f"Erro na solicitação: {response.status_code}")
        print(response.json())

fetch_insights()

# Fechar a conexão com o banco de dados
cursor.close()
conexao.close()