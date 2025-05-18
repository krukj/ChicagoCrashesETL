
# person_key - sztuczny klucz główny (jeszcze nie wiem czy tak to rozwiązujemy)
# PERSON_ID - mamy to w danych jakieś stringi to są typu O749947
# PERSON_TYPE - str (jeśli null UNKNOWN ale nie mo)
# mamy CRASH_RECORD_ID to trzeba będzie jakoś połączyć z CrashFact (jakiś hash)
# mamy też VEHICLE_ID (można to jakoś uwzględnić idk jeszcze) (to floaty ale chyba w praktyce inty większość albo wszystkie)
# SEX - M/F/X -> X zmieniamy na UNKNOWN
# AGE - floaty -> zamieniamy na inty ofc (tylko sprawdzić czy git wszystko) oraz 29% nulli -> nie wiem chyba NULL bo nie 0 na pewno i nie wiem czy jest sens jakieś 999
# SAFETY_EQUIPMENT - str (jak null to "USAGE UNKNOWN")
# AIRBAG_DEPLOYED - str (jak null to "DEPLOYMENT UNKNOWN")
# EJECTION - str (jak null to "UNKNOWN")
# INJURY_CLASSIFICATION - str (jak null to "UNKNOWN")
# DRIVER_ACTION - str (jak null to "UNKNOWN")
# DRIVER_VISION - str (jak null to "UNKNOWN")
# PHYSICAL_CONDITION - str (jak null to "UNKNOWN")
# BAC_RESULT - str (jak null to "UNKNOWN")
# BAC_RESULT VALUE - float -> nulli jest 99% (wtedy chyba też NULL, raczej to jest jak jest "TEST NOT OFFERED")

