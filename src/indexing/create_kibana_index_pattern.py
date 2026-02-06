import requests
import sys

def create_index_pattern(kibana_url: str):
    """Crée automatiquement l'index pattern dans Kibana"""
    
    headers = {
        "kbn-xsrf": "true",
        "Content-Type": "application/json"
    }
    
    payload = {
        "attributes": {
            "title": "football_value_index-*",
            "timeFieldName": "event_time"
        }
    }
    
    url = f"{kibana_url}/api/saved_objects/index-pattern/football_value_index"
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code in [200, 409]:  # 409 = déjà existant
            print(f"✅ Index pattern créé ou déjà existant")
            return True
        else:
            print(f"❌ Erreur création index pattern: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Erreur connexion Kibana: {e}")
        return False


if __name__ == "__main__":
    kibana_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:5601"
    create_index_pattern(kibana_url)
