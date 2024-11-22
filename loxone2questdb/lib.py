

def fixKeyValue(key, value):
    if value == None:
        return None, None
    value = fix_value(value)
    if(isinstance(value, (int, float))):
        return key, value
    else:
        key = f"{key}_str"
        value = str(value)
        return key, value

# pokusi sa previest value na float. pokial sa to nepodari, prevedie ho na string a modifikuje nazov fieldu (prida _str) tak, aby nedoslo ku konfliktu typov v databazi
def fixColumns(columns):
    ret = {}
    for key in columns:
        value = columns[key]
        key, value = fixKeyValue(key, value)
        if key is not None and value is not None:
            ret[key] = value
    return ret


# prevedie hodnotu na cislo alebo string. U hodnot kde je cislo a string (napriklad "1.0 kW") sa pokusi extrahovat 1.0 ako cislo
# vrati povodnu hodnotu ak sa konverzia nepodarila
def fix_value(value):
    if isinstance(value, (int, float)):
        return float(value)
    else:
        s = str(value)
        if s.lower() in ["true", "on", "zap", "ano", "yes", "1"]:
            return 1
        elif s.lower() in ["false", "off", "vyp", "ne", "no", "0"]:
            return 1
        if " " in s:
            words = s.split(" ")
            s = words[0].strip()
        # odzadu odmazava znaky, kym nenarazi na cislo (odreze kW, W, %, atd.)
        while s and not s[-1].isdigit():
            s = s[:-1]
        try:
            return  float(s)
        except ValueError:
            return value
