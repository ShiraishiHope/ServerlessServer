import logging
import json
import os
import io
import pandas as pd
import statistics
import csv

from azure.functions import InputStream, Out
from azure.storage.blob import BlobServiceClient


def main(myblob: InputStream):

    logging.info("Function?????")

    # Initialize BlobServiceClient with your storage connection string
    connection_string = os.environ.get('storagebros_STORAGE')
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    # Lire le contenu du fichier CSV
    contenu_fichier = myblob.read().decode("latin-1")
    logging.log(contenu_fichier)

    dfCsv=pd.read_csv(contenu_fichier,skiprows=1)
    columns=["ID","Nom","Prix","Quantité","Note_Client"]
    dfCsv.columns = columns  # Assigne la preière ligne comme noms de colonnes

    MedianPrix=(dfCsv['Prix']).median()
    MoyenPrix=(dfCsv['Prix']).mean()
    EcartTypePrix= statistics.stdev(dfCsv['Prix'])

    MedianNotes=(dfCsv['Note_Client']).median()
    MoyenNotes=(dfCsv['Note_Client']).mean()
    EcartTypeNotes= statistics.stdev(dfCsv['Note_Client'])

    MedianQuantité=(dfCsv['Quantité']).median()
    MoyenQuantité=(dfCsv['Quantité']).mean()
    EcartTypeQuantité=statistics.stdev(dfCsv['Quantité'])

    AnomaliePrix= (~dfCsv['Prix'].between(0,500)).sum() /1.0
    AnomaliePrixLignes= dfCsv[(~dfCsv['Prix'].between(0,500))].apply(lambda row: ';'.join(row.astype(str)), axis=1).tolist()
    print(AnomaliePrixLignes)
  

    AnomalieNotes= (~dfCsv['Note_Client'].between(0,5)).sum() / 1.0
    AnomalieNotesLignes= dfCsv[(~dfCsv['Note_Client'].between(0,5))].apply(lambda row: ';'.join(row.astype(str)), axis=1).tolist()
    print(AnomalieNotesLignes)

    AnomalieQuantite= (~dfCsv['Quantité'].between(0,1000)).sum() / 1.0
    AnomalieQuantiteLignes= dfCsv[(~dfCsv['Quantité'].between(0,1000))].apply(lambda row: ';'.join(row.astype(str)), axis=1).tolist()
    print(AnomalieQuantiteLignes)

    # Création du dictionnaire pour le rapport
    rapport = {
            "statistiques": {
                "prix": {
                    "mediane": MedianPrix,
                    "moyenne": MoyenPrix,
                    "ecart-type": EcartTypePrix
                },
                "note_client": {
                    "mediane": MedianNotes,
                    "moyenne": MoyenNotes,
                    "ecart-type": EcartTypeNotes
                },
                "quantite": {
                    "mediane": MedianQuantité,
                    "moyenne": MoyenQuantité,
                    "ecart-type": EcartTypeQuantité
                }
            },
            "anomalies": {
                "prix": {
                    "anomalies_count": AnomaliePrix,
                    "lignes_anomalies": AnomaliePrixLignes
                },
                "note_client": {
                    "anomalies_count": AnomalieNotes,
                    "lignes_anomalies": AnomalieNotesLignes
                },
                "quantite": {
                    "anomalies_count": AnomalieQuantite,
                    "lignes_anomalies": AnomalieQuantiteLignes
                }
            }
        }
    # Convert dictionary to JSON string
    json_content = json.dumps(rapport, indent=2)
        
    # Use the binding paths from function.json
    container_name = "json-report"
    blob_name = myblob.name
    new_name = blob_name.replace("csv", "json")
        
    # Get container client
    container_client = blob_service_client.get_container_client(container_name)
        
    # Get blob client
    blob_client = container_client.get_blob_client(new_name)
        
    # Upload the JSON content
    logging.info(f"Writing JSON to {container_name}/{new_name}")
    blob_client.upload_blob(
        json_content,
        overwrite=True,
        encoding='utf-8')

         # Convert dictionary to JSON string
        #logging.info(f"Writing JSON to json-report/{new_name}")
        
        # Write to output binding
        #outputjson.set(json_content)
        
     #except Exception as ex:
      #  logging.error(f"Error processing blob: {str(ex)}")
       # raise

