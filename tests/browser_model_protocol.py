"""Scripted external-provider fixtures read the real native browser schema."""
import json


def browser_schema_messages(kwargs):
    return [{'role':'user','content':'Return valid JSON matching this schema: '+json.dumps(tool['input_schema'])}
        for tool in kwargs.get('tools',[]) if tool.get('name') == 'browser_decision']
