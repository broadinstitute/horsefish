import sys

class EmptySchemaDefinitionException(Exception):
     print("Exception occurred: schema definition in config.JSON was found to be empty.")
     sys.exit()
    