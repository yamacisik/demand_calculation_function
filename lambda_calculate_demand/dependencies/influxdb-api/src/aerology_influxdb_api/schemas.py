
configuration_json_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "influx": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string"
                },
                "token": {
                    "type": "string"
                },
                "org": {
                    "type": "string"
                }
            },
            "required": [
                "url",
                "token",
                "org"
            ]
        },
        "airports": {
            "type": "array",
            "items": [
                {
                    "type": "object",
                    "properties": {
                        "short_name": {
                            "type": "string"
                        },
                        "influx_bucket": {
                            "type": "string"
                        },
                        "lane_names": {
                            "type": "array",
                            "items": [
                                {
                                    "type": "string"
                                },
                                {
                                    "type": "string"
                                },
                                {
                                    "type": "string"
                                }
                            ]
                        }
                    },
                    "required": [
                        "short_name",
                        "influx_bucket",
                        "lane_names"
                    ]
                }
            ]
        }
    },
    "required": [
        "influx",
        "airports"
    ]
}
