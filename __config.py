

import collections
def config_update(config, updates):
    # update the key-values in config with the key-values in updates
    for config_key, config_val in updates.items():
        if isinstance(config_val, collections.Mapping):
            config[config_key] = config_update(config.get(config_key, {}), config_val)
        else:
            config[config_key] = config_val
    return config

def config_replace_placeholders(config, placeholders):
    # with in each key-value string, replace the placeholder key with the placeholder value
    for placeholder_key, placeholder_val in placeholders.items():
        if isinstance(placeholder_val, str):
            for config_key, config_val in config.items(): 
                if isinstance(config_val, str):
                    config[config_key] = config_val.replace('{%s}' % placeholder_key, placeholder_val)
                if isinstance(config_val, collections.Mapping):
                    config[config_key] = config_replace_placeholders(config_val, {placeholder_key : placeholder_val})
    return config
