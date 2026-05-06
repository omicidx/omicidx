class BaseType(object):
    def __init__(self, name, nullable):
        self.name = name
        self.nullable = nullable
    
    def __repr__(self):
        return "<{} class name:{}>".format(self.__type__,self.name)


################################################
#
#
#  Classes for simple entities:
#
#  Strings, Dates(timestamp), Boolean,
#  Long, Integer
#
#
################################################

    
class StringType(BaseType):
    __type__ = 'StringType'
    def __init__(self, name, nullable):
        super().__init__(name, nullable)
        
    def mapping(self):
        return({
                    "fields": {
                      "keyword": {
                        "ignore_above": 256,
                        "type": "keyword"
                      }
                    },
                    "type": "text"
        })
        
class DateType(BaseType):
    __type__ = 'DateType'
    def __init__(self, name, nullable):
        super().__init__(name, nullable)
        
    def mapping(self):
        return({
            "type": "timestamp"
          }
        )

class BooleanType(BaseType):
    __type__ = 'BooleanType'
    def __init__(self, name, nullable):
        super().__init__(name, nullable)
        
    def mapping(self):
        return({
            "type": "boolean"     
        })

class LongType(BaseType):
    __type__ = 'LongType'
    def __init__(self, name, nullable):
        super().__init__(name, nullable)
    
    def mapping(self):
        return({
            "type": "long"
          
        })
        
        
class IntType(BaseType):
    __type__ = 'IntType'
    def __init__(self, name, nullable):
        super().__init__(name, nullable)
        
    def mapping(self):
        return({
            "type": "integer"
          
        })

################################################
#
#
#  Classes for complex entities:
#
#    Objects
#    Nested (arrays)
#
#
################################################

class ObjectType(BaseType):
    # spark type: struct
    # ES type:    object
    __type__ = 'ObjectType'
    def __init__(self, name, nullable):
        super().__init__(name, nullable)
        self.fields = {}

    def mapping(self):
        return({
            "properties": dict((k, self.fields[k].mapping()) for k in self.fields.keys())
        })        
    
        
class NestedType(BaseType):
    # spark type: array
    # ES type:    nested
    __type__ = 'NestedType'
    def __init__(self, name, nullable):
        super().__init__(name, nullable)
        self.fields = {}
        
    def mapping(self):
        return({
            "include_in_parent": True,
            "type": "nested",
            "properties": dict((k, self.fields[k].mapping()) for k in self.fields.keys())
        })

def walk_schema(schema, parent = None):
    """Walk a pyspark "schema.jsonValue" schema

    This function converts a pyspark schema into
    a set of classes that can then convert the schema
    to an elasticsearch mapping.

    Parameters
    ----------
    schema: a dict
        Typically, this would come from `df.schema.jsonValue()`. It could
        also be loaded from a string or file using the `json` library.
    parent: a container class, so an `ObjectType` or a `NestedType`
        If empty, will be initialized with an `ObjectType` with 
        name = "root".

    >>> s = walk_schema(df.schema.jsonValue(), ObjectType(name='root'))
    >>> s.mapping()
    """

    if(parent is None):
        parent = ObjectType(name = 'root')

    for field in schema['fields']:
        if(field['type'] == 'string'):
            parent.fields[field['name']]=StringType(name=field['name'], nullable=field['nullable'])
        if(field['type'] == 'integer'):
            parent.fields[field['name']]=IntType(name=field['name'], nullable=field['nullable'])
        if(field['type'] == 'long'):
            parent.fields[field['name']]=LongType(name=field['name'], nullable=field['nullable'])
        if(field['type'] == 'timestamp'):
            parent.fields[field['name']]=DateType(name=field['name'], nullable=field['nullable'])
        if(field['type'] == 'boolean'):
            parent.fields[field['name']]=BooleanType(name=field['name'], nullable=field['nullable'])
        if(type(field['type']) == dict):
            n_field = field['type']
            if(n_field['type'] == 'struct'):
                newp = ObjectType(field['name'], field['nullable'])
                parent.fields[field['name']]=walk_schema(n_field, parent=newp)

            if(n_field['type'] == 'array'):
                newp = NestedType(field['name'], field['nullable'])
                parent.fields[field['name']]=walk_schema(n_field['elementType'], parent=newp)
    return parent
