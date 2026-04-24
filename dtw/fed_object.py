from dataclasses import dataclass

# from ray import ObjectRef

# @dataclass
# class Party:
#     host:str
#     port:int

@dataclass
class DtwObject:
    uuid:str
    host:str
    port:int