"""
Firestore Client Module
Provides a singleton Firestore client for the application.
Uses the same GCP project credentials as BigQuery.
"""

import firebase_admin
from firebase_admin import credentials, firestore
import os

# Initialize Firebase Admin SDK with the same credentials as BigQuery
_firebase_app = None
_db = None

def get_firestore_client():
    """
    Returns a Firestore client instance.
    Initializes Firebase Admin SDK on first call.
    """
    global _firebase_app, _db
    
    if _db is not None:
        return _db
    
    # Use the same credentials file as BigQuery (for local development)
    cred_path = os.environ.get(
        'GOOGLE_APPLICATION_CREDENTIALS', 
        'routers/neo-rank-tracker-63b755f3c88a.json'
    )
    
    try:
        # Check if credentials file exists (local development)
        if os.path.exists(cred_path):
            cred = credentials.Certificate(cred_path)
            _firebase_app = firebase_admin.initialize_app(cred)
            print("Firestore client initialized with local credentials file")
        else:
            # Cloud Run: use Application Default Credentials
            _firebase_app = firebase_admin.initialize_app()
            print("Firestore client initialized with Application Default Credentials")
        _db = firestore.client()
    except ValueError:
        # App already initialized
        _db = firestore.client()
    
    return _db

# Convenience function to get collections
def get_users_collection():
    """Returns reference to the 'users' collection"""
    return get_firestore_client().collection('users')

def get_projects_collection():
    """Returns reference to the 'projects' collection"""
    return get_firestore_client().collection('projects')
