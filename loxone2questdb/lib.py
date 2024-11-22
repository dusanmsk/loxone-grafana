

# pokusi sa previest value na float. pokial sa to nepodari, prevedie ho na string a modifikuje nazov fieldu (prida _str) tak, aby nedoslo ku konfliktu typov v databazi
def fixColumns(columns):
    ret = {}
    for key, value in columns:
        success, fixed_value = fix_value(value)
        if success:
            value = fixed_value
        else:
            key = f"{key}_str"
            value = str(value)
            if value == 'None':
                value = None
        ret[key] = value
    return ret

# prevedie hodnotu na cislo alebo string. U hodnot kde je cislo a string (napriklad "1.0 kW") sa pokusi extrahovat 1.0 ako cislo
# vrati [bool, fixed_value] ako odpoved. pokial je bool false, tak sa konverzia nepodarila a musi sa to ulozit ako string
def fix_value(value):
    if isinstance(value, (int, float)):
        return True, float(value)
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
            return True, float(s)
        except ValueError:
            return False, value
