import argparse
from google.cloud import storage as gcs
import json
from utils import *


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Clear all incident banners on Terra UI.')

    parser.add_argument('--env', type=str, required=True,
                        help='"prod" or "dev" Terra environment for banner.')
    
    # template json text for banner deletion
    clear_banner_text = "[]"

    # push json string to bucket - clear banner
    push_service_banner_json(env, clear_banner_text)
