import yaml

with open("config.yml", 'r', encoding='utf-8') as ymlfile:
    cfg = yaml.load(ymlfile)