source('C:/git/cqapiR/functions.R')

get_concepts("adb_energie")


exit(1)
dataset = "adb_energie"
cq_url = "http://lyo-peva02.spectrumk.ads:8020"
cq_url = "http://lyo-peva01.spectrumk.ads:8080"
cq_token = login("initial_user@ingef.de")
cq_token = "dKRILd5JEKwBSuxJ0DhK/ONzy60wPkY7FzpaQQ+WGXOSsR0JB5gl/IPohUmbcC3R/3MLhg6zxDZiD0KjqdnJfF4o0zW7gBBvsd7ZZ/vR22aHyzOrY4813xtfdxMZFnVJUTllPiM4CeU2JFJdK6pznfehc4refCQO1onpR7QJW5d/8LqJrtThPTizZFCCSoFEK94zpWbTRtLgdKhPBQvSgaz6WzPM7+9lpqHRCTESwdUrjSJLj0zDtXeh9V482gSMdT6DiCIxonI0+YlCYLNref1dhMdG2ok2xw/FUK7Cp7hMKDyHwVm72CB+CN9a3vba"

query_id = "adb_bosch.90365c22-4e0a-4a15-aad0-46f0429267e2"

query_info = get_query_info(query_id)
data = get_query_result(query_id)

stored_queries = get_stored_queries("adb_bosch")
get_query_result(query_label_to_id("adb_bosch", "testikus"))

query = fromJSON('{
    "type": "CONCEPT_QUERY",
    "root": {
        "type": "AND",
        "children": [
            {
                "type": "DATE_RESTRICTION",
                "dateRange": {
                    "min": "2017-01-01",
                    "max": "2017-12-31"
                },
                "child": {
                    "type": "OR",
                    "children": [
                        {
                            "type": "CONCEPT",
                            "ids": [
                                "adb_bosch.icd.c00-d48.c43-c44.c43.c43_0"
                            ],
                            "label": "C43.0",
                            "tables": [
                                {
                                    "id": "adb_bosch.icd.kh_diagnose_icd_code",
                                    "dateColumn": {
                                        "value": "adb_bosch.icd.kh_diagnose_icd_code.entlassungsdatum"
                                    },
                                    "selects": [],
                                    "filters": []
                                },
                                {
                                    "id": "adb_bosch.icd.au_fall",
                                    "dateColumn": {
                                        "value": "adb_bosch.icd.au_fall.au-beginn"
                                    },
                                    "selects": [],
                                    "filters": []
                                },
                                {
                                    "id": "adb_bosch.icd.arzt_diagnose_icd_code",
                                    "dateColumn": null,
                                    "selects": [],
                                    "filters": []
                                },
                                {
                                    "id": "adb_bosch.icd.au_fall_21c",
                                    "dateColumn": {
                                        "value": "adb_bosch.icd.au_fall_21c.au-beginn_(21c)"
                                    },
                                    "selects": [],
                                    "filters": []
                                }
                            ],
                            "selects": []
                        }
                    ]
                }
            }
        ]
    }
}')

connection = list(
  url="http://lyo-peva01.spectrumk.ads:8080",
  token=NULL,
  dataset="adb_energie"
)
execute_query("adb_bosch", query)
get_query_result(execute_query("adb_energie", concept_to_query("adb_energie.alter", start_date="2017-01-01", end_date="2017-01-31")))

