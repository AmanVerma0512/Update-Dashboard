#!/usr/bin/env python
# coding: utf-8

# In[24]:


import firebase_admin
from firebase_admin import credentials, firestore
from more_itertools import peekable
import json
import boto3
import streamlit as st


# In[25]:


class DataPreprocessor():
    def initializeFirebaseApp(self):
        config = {
            "type": st.secrets["type"],
            "project_id": st.secrets["project_id"],
            "private_key_id": st.secrets["private_key_id"],
            "private_key": st.secrets["private_key"],,
            "client_email": st.secrets["client_email"],
            "client_id": st.secrets["client_id"],
            "auth_uri": st.secrets["auth_uri"],
            "token_uri": st.secrets["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["client_x509_cert_url"]
        }

        cred = credentials.Certificate(config)

        firebase_admin.initialize_app(cred)

    def createClient(self):
        self.db = firestore.client()

    def getCollectionData(self, collectionRef):
        print("getCollectionData")
        docs = []

        for doc in collectionRef.stream():
            data = doc.to_dict()
            data['id'] = doc.id
            print(doc.id)

            docs.append(data)

        return docs

    def getData(self, collectionId):
        print("getData")
        return self.getCollectionData(self.db.collection(collectionId))

    def listSubcollections(self, collectionId, documentId):
        print("listSubcollections")
        collections = self.db.collection(collectionId).document(documentId).collections()

        collectionIterator = peekable(collections)

        if not collectionIterator:
            return None
        
        return collectionIterator

    def getSubcollectionData(self, collectionId, documentId):
        print("getSubcollectionData")
        collections = self.listSubcollections(collectionId, documentId)

        if collections == None:
            return None

        subCollectionData = {}

        for collection in collections:
            subCollectionData[collection.id] = []

            for doc in collection.stream():
                document = doc.to_dict()
                document['id'] = doc.id

                subCollectionData[collection.id].append(document)

        return subCollectionData

    
    def getDataDump(self, collectionId):
        print("getDataDump")
        collection_data = self.getData(collectionId)

        subcollection_data = {}
        for data in collection_data:
            data_id = data['id']
            subcollection_data[data_id] = {}

            subcollection = self.getSubcollectionData(collectionId, data_id)

            if subcollection != None:
                subcollection_data[data_id] = subcollection

        return collection_data, subcollection_data

    def getCombinedDataDump(self, collectionId):
        print("getCombinedDataDump")
        collection_data, subcollection_data = self.getDataDump(collectionId)

        print("Fetched main collection data")
        combined_data = {}
        for doc in collection_data:

            print(doc['id'])
            doc_id = doc['id']

            subcollection_docs = subcollection_data[doc_id]

            if len(subcollection_docs)!=0:
                combined_data[doc_id] = doc
                combined_data[doc_id]['subcollections'] = subcollection_docs

        return combined_data

    def restructureData(self, combinedData):

        data = []
        for user in combinedData:

            try:
                print(user)
                user_data = {}
                user_data['id'] = user
                user_data['name'] = combinedData[user]['name']
                user_data['exercises'] = {}
                user_exercise_data = combinedData[user]['subcollections']

                for exercise_name in user_exercise_data:
                    exercise_data_list = user_exercise_data[exercise_name]

                    for exercise in exercise_data_list:
                        exercise_date = exercise['id']

                        if not exercise_date in user_data['exercises'].keys():
                            user_data['exercises'][exercise_date] = []
                        user_data['exercises'][exercise_date].append(exercise)    

                data.append(user_data)             

            except:
                continue  

        return data

    def removeNewLine(self, array):
        strArray = str(array)

        strArray.replace(",\n", " ")

        strArray = strArray[1:-1]

        strArray = list(strArray.split(","))

        return strArray

    def getDataDumpAndRestructure(self, collectionId):
        combined_data = self.getCombinedDataDump(collectionId)

        restructured_data = self.restructureData(combined_data)

        return restructured_data

    def getJarvisData(self):
        workouts_completed_ref = self.db.collection('companies').document('Jarvis').collection('WORKOUTS COMPLETED')

        docs = self.getCollectionData(workouts_completed_ref)

        docs = self.restructureJarvisData(docs)
        
        return docs

    def restructureJarvisData(self, data):
        jarvis_data = {}

        for doc in data:
            jarvis_data[doc['id']] = doc
    
        return jarvis_data

    def saveData(self, data, filename):

        json_obj = json.dumps(data, indent = 2)
        with open(filename, "w") as fw:
            fw.write(json_obj)


# In[26]:


st.title("Update Data for Dashboard")

if st.button("Update Data"):
    
    preprocessor = DataPreprocessor()
    preprocessor.initializeFirebaseApp()
    preprocessor.createClient()
    data = preprocessor.getDataDumpAndRestructure('users')
    st.write("Firebase Data Loaded and initialised!")
    
    s3 = boto3.resource('s3', aws_access_key_id = st.secrets["aws_access_key_id"], aws_secret_access_key = st.secrets["aws_secret_access_key"])
    object = s3.Object('forgefait', 'new_data.json')
    object.put(Body=str(data))
    content_object = s3.Object('forgefait', 'new_data.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
#     p = re.compile('(?<!\\\\)\'')
#     file_content = p.sub('\"', file_content)
#     json_data = json.loads(file_content)
    st.write("Data is Changed! Updated in s3 Bucket!")
    st.write("Process Completed!")    
#     if data != json_data:
#         st.write("Data is Changed! Updated in s3 Bucket!")
#         st.write("Process Completed!")
#     else:
#         st.write("Data is Same! Updated in s3 Bucket!")
#         st.write("Process Completed!")
