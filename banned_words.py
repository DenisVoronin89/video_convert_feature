""" Словарь для первичной проверки хэштегов на мат и юридически запрещенные слова"""

BANNED_WORDS = {
    # Маты и оскорбления
    "хуй", "пизда", "ебать", "сука", "блядь", "гандон", "мразь", "сучка",
    "пидор", "нахуя", "нахуй", "ебаный", "ебанутый", "хер", "пиздец",
    "дрянь", "тварь", "говно", "мудак", "ублюдок", "сволочь", "чмо",
    "дебил", "кретин", "идиот", "тупица", "мразота", "курва", "шалава",
    "блядина", "долбоеб", "очко", "падла", "ссать", "гавно", "манда",
    "задница", "жопа", "срать", "трахать", "влагалище", "анус", "проститутка",
    "нахера", "заебал", "охуел", "пидрила", "шлюха", "ебись", "петух",
    "выебон", "гопник", "дебик", "обмудок", "ссанина", "выродок", "задрот",
    "кретинка", "очкарик", "уродина",

    # Наркотики и связанные термины
    "наркота", "травка", "дурь", "герыч", "шмаль", "косяк", "гашиш",
    "спайс", "соль", "миксы", "грибы", "кокс", "фен", "лсд", "кислота",
    "план", "дурман", "марихуана", "меф", "мет", "нарик", "торчок",
    "ширка", "пластилин", "кайф", "барбитураты", "винт", "убитый",
    "трип", "отходосы", "колёса", "закладка", "чек", "зелье", "таблы",
    "вмазка", "амф", "марафет", "кайфануть", "ширнуться", "раствор",
    "растворчик", "малина", "ханка", "опий", "героин", "порошочек",
    "транк", "сняться", "курево", "драп", "солома", "кумар",

    # Оружие и насилие
    "ствол", "волына", "огнестрел", "глок", "калаш", "дробовик",
    "травмат", "обрез", "сайга", "автомат", "ружьё", "винторез",
    "рпг", "тт", "боеприпасы", "патроны", "нож", "заточка", "кинжал",
    "арсенал", "бомбочка", "граната", "снайперка", "арбалет", "катана",
    "молотов", "самопал", "стволяр", "снайпер", "заряд", "подрывник",
    "взрывчатка", "теракт", "заложник", "погром", "штурм", "нападение",
    "убийца", "насильник", "маньяк", "убийство", "грабёж", "вымогательство",
    "разбой", "пытка", "пытать", "расчленение", "изнасилование",
    "разборки", "мясо", "поножовщина", "перестрелка", "разнос",
    "залп", "драка", "завалить", "пристрелить", "зарезать", "удушение",
    "морг", "пропажа", "захват", "повстанец", "диверсант", "убить",
    "нанести",

    # Проституция и сексуальный контент
    "шлюха", "проститутка", "интим", "бордель", "минет", "анальный",
    "порно", "групповуха", "оргия", "жрица", "камшот", "куни",
    "жмж", "секс-шоп", "мастурбация", "секс-клуб", "гей-порно",
    "лямур", "бдсм", "стрептиз", "рабыня", "секс-игрушки", "вибратор",
    "страпон", "мастурбация", "доминирование", "разврат", "секс-работник",
    "фетиш", "петтинг", "фистинг", "нагота", "эротика", "интимка",
    "проститута", "эскорт", "девушка по вызову", "трансуха", "порево",
    "эротическое",

    # Криминал и мошенничество
    "воровство", "казнокрадство", "угон", "контрафакт", "нелегал",
    "подделка", "фальшивка", "аферист", "обман", "махинация", "шулер",
    "рэкет", "рейдер", "наёмник", "заказуха", "похищение", "коррупция",
    "взятка", "кардер", "скимер", "чёрный рынок", "дилер", "трафик",
    "спам", "фишинг", "мошенник", "анонимайзер", "развод", "закладчик",
    "контрабанда", "наркоторговля", "работорговля", "пиратство",
    "нелегальный", "спекулянт", "гоп-стоп", "погромщик", "разбойник",
    "налёт", "криминалитет", "дезертир", "вор", "карманник", "вымогатель",
    "рэкетир", "угонщик", "налётчик"
    
# English offensive words
    "fuck", "shit", "bitch", "bastard", "asshole", "damn", "cunt",
    "pussy", "dick", "cock", "fucking", "motherfucker", "son of a bitch",
    "slut", "whore", "douchebag", "faggot", "wanker", "tosser", "twat",
    "bollocks", "arse", "moron", "idiot", "scumbag", "jerk", "loser",
    "dumbass", "retard", "prick", "dipshit", "jackass", "shithead",
    "shitface", "dickhead", "ballsack", "nutjob", "pecker", "screwball",

    # Drugs and narcotics
    "weed", "pot", "grass", "coke", "heroin", "meth", "ecstasy",
    "lsd", "acid", "crack", "hash", "mary jane", "molly", "dope",
    "smack", "speed", "tripping", "opium", "barbiturate", "amphetamine",
    "tranquilizer", "sedative", "xanax", "valium", "adderall", "ketamine",
    "mushrooms", "shrooms", "duster", "nitrous", "glue sniffing",
    "drug dealer", "narco", "stash", "high", "stoned", "bong",
    "blunt", "joint", "pipe", "hit", "line", "trip", "tab", "pill",
    "overdose", "od", "needle", "syringe", "inject", "snort", "roll",

    # Weapons and violence
    "gun", "knife", "bomb", "grenade", "rpg", "rocket launcher",
    "pistol", "revolver", "shotgun", "sniper", "assault rifle", "ak47",
    "glock", "machete", "katana", "chainsaw", "molotov", "explosives",
    "ammunition", "bullet", "shell", "missile", "landmine", "terrorist",
    "attack", "kill", "murder", "rape", "stab", "shoot", "choke",
    "strangle", "beat", "bombing", "hostage", "torture", "execution",
    "manslaughter", "assassination", "arson", "riot", "massacre",
    "genocide", "homicide", "lynching", "mass shooting", "terrorism",
    "car bomb", "IED", "booby trap", "silencer", "tasers", "baton",
    "riot shield", "armory", "deadly", "attackers", "criminals",

    # Prostitution and sexual content
    "prostitute", "escort", "call girl", "hooker", "brothel", "porn",
    "porno", "pornography", "erotic", "bdsm", "fetish", "dominatrix",
    "lap dance", "stripper", "strip club", "peep show", "sex shop",
    "vibrator", "dildo", "sex toy", "bondage", "orgy", "threesome",
    "handjob", "blowjob", "anal", "oral", "cum", "cumshot", "gangbang",
    "nude", "naked", "masturbate", "masturbation", "erotica", "underage",
    "child porn", "sex trafficking", "sexual abuse", "grooming", "pervert",
    "voyeur", "flasher", "rape fantasy", "incest", "pedophile", "paedophile",
    "non-consensual", "molest", "molestation", "rape", "abuse", "exploit",

    # Crime and fraud
    "scam", "fraud", "phishing", "hacking", "piracy", "theft",
    "burglary", "arson", "vandalism", "counterfeit", "black market",
    "illegal", "contraband", "money laundering", "bribery", "extortion",
    "smuggling", "trafficking", "embezzlement", "racketeering",
    "kidnapping", "ransom", "assassination", "spyware", "malware",
    "adware", "blackmail", "insider trading", "cybercrime", "hacker",
    "spy", "thief", "burglar", "arsonist", "forgery", "fake id",
    "fake passport", "identity theft", "credit card fraud", "tax evasion",
    "cyber attack", "cyber warfare", "phish", "carding", "skimming",
    "trafficker", "drug cartel", "money mule", "criminal network",
    "organized crime"
}
