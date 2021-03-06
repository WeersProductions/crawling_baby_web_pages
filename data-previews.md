```
hdfs dfs -text /data/doina/WebInsight/2020-07-13/1M.2020-07-13-aa.gz | head -1
```

Schema:
```
root
 |-- _corrupt_record: string (nullable = true)
 |-- fetch: struct (nullable = true)
 |    |-- contentDigest: string (nullable = true)
 |    |-- contentLength: long (nullable = true)
 |    |-- depth: long (nullable = true)
 |    |-- externalLinks: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- linkInfo: struct (nullable = true)
 |    |    |    |    |-- linkQuality: double (nullable = true)
 |    |    |    |    |-- linkRels: array (nullable = true)
 |    |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |    |-- linkType: string (nullable = true)
 |    |    |    |    |-- text: string (nullable = true)
 |    |    |    |-- targetUrl: string (nullable = true)
 |    |-- fetchDate: string (nullable = true)
 |    |-- fetchDuration: long (nullable = true)
 |    |-- fetchStatus: string (nullable = true)
 |    |-- fetchTimeToFirstByte: long (nullable = true)
 |    |-- httpStatus: long (nullable = true)
 |    |-- internalLinks: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- linkInfo: struct (nullable = true)
 |    |    |    |    |-- linkQuality: double (nullable = true)
 |    |    |    |    |-- linkRels: array (nullable = true)
 |    |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |    |-- linkType: string (nullable = true)
 |    |    |    |    |-- text: string (nullable = true)
 |    |    |    |-- targetUrl: string (nullable = true)
 |    |-- ipAddress: string (nullable = true)
 |    |-- isFinalized: boolean (nullable = true)
 |    |-- language: string (nullable = true)
 |    |-- metadata: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- key: string (nullable = true)
 |    |    |    |-- values: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |-- parsingErrors: long (nullable = true)
 |    |-- removedExternalLinks: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- removedInternalLinks: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- robotsTag: struct (nullable = true)
 |    |    |-- NOARCHIVE: boolean (nullable = true)
 |    |    |-- NOFOLLOW: boolean (nullable = true)
 |    |    |-- NOINDEX: boolean (nullable = true)
 |    |    |-- NOSNIPPET: boolean (nullable = true)
 |    |-- semanticVector: string (nullable = true)
 |    |-- textQuality: double (nullable = true)
 |    |-- textSize: long (nullable = true)
 |    |-- title: string (nullable = true)
 |-- history: struct (nullable = true)
 |    |-- changeCount: long (nullable = true)
 |    |-- fetchCount: long (nullable = true)
 |    |-- previousFetches: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |-- url: string (nullable = true)
 |-- urlViewInfo: struct (nullable = true)
 |    |-- lastFetch: string (nullable = true)
 |    |-- lastSchedule: string (nullable = true)
 |    |-- lastView: string (nullable = true)
 |    |-- metrics: struct (nullable = true)
 |    |    |-- entries: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- metrics: struct (nullable = true)
 |    |    |    |    |    |-- entries: array (nullable = true)
 |    |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |    |    |    |-- value: double (nullable = true)
 |    |    |    |    |-- scope: string (nullable = true)
 |    |-- numInLinksExt: string (nullable = true)
 |    |-- numInLinksInt: string (nullable = true)
```

Raw:
```
{
  "fetch": {
    "depth": 0,
    "language": "de",
    "textQuality": 1.497677,
    "contentLength": 52686,
    "httpStatus": 200,
    "title": "[A CENSORED WEBSITE TITLE]",
    "fetchDuration": 2058,
    "fetchDate": "2020-07-13 22:22",
    "parsingErrors": 0,
    "robotsTag": {
      "NOINDEX": false,
      "NOFOLLOW": false,
      "NOARCHIVE": false,
      "NOSNIPPET": false
    },
    "internalLinks": [
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "LINK",
          "linkRels": ["SHORTLINK"],
          "text": "",
          "linkQuality": 0.99308187
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "LINK",
          "linkRels": ["ALTERNATE"],
          "text": "",
          "linkQuality": 0.99183744
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "LINK",
          "linkRels": ["ALTERNATE"],
          "text": "",
          "linkQuality": 0.9915631
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "LINK",
          "linkRels": ["ALTERNATE"],
          "text": "",
          "linkQuality": 0.97252625
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "LINK",
          "linkRels": ["ALTERNATE"],
          "text": "",
          "linkQuality": 0.97162145
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "IMG",
          "linkRels": [],
          "text": "fuma fachstelle gender \u0026 diversität",
          "linkQuality": 0.96770453
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "unsere vision",
          "linkQuality": 0.96555483
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "unser imagefilm",
          "linkQuality": 0.9644288
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "geschlechtergerechte sprache",
          "linkQuality": 0.9632674
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "stellenangebote",
          "linkQuality": 0.9620696
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "fuma e.v.",
          "linkQuality": 0.96083426
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "team",
          "linkQuality": 0.95956045
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "fuma fortbildungen",
          "linkQuality": 0.9568927
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "rassismuskritik \u0026 kritisches weißsein",
          "linkQuality": 0.9540575
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "rassismus, männlichkeiten*, empowerment",
          "linkQuality": 0.95257413
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "der anti-bias-ansatz",
          "linkQuality": 0.95104533
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "discover diversity island",
          "linkQuality": 0.94946986
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "bodyismuskritische pädagogik",
          "linkQuality": 0.94617385
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "#connect qualifizierungsreihe",
          "linkQuality": 0.94445074
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "digitale lernwelten",
          "linkQuality": 0.9426758
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "fuma – talks \u0026 web-seminare",
          "linkQuality": 0.94084775
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "colorism",
          "linkQuality": 0.93896514
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "sexualisierte gewalt durch kolleg_innen",
          "linkQuality": 0.9370266
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "fuma lernkarten",
          "linkQuality": 0.9350308
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "fuma \u0026 friends",
          "linkQuality": 0.9329763
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "selbstlernkurs ‘jump in’",
          "linkQuality": 0.9308616
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "#bit* basics inter* und trans*",
          "linkQuality": 0.92868525
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "fuma-erklärvideos",
          "linkQuality": 0.92644584
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "#connect community",
          "linkQuality": 0.9241418
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "inhouse / on demand",
          "linkQuality": 0.92177176
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "fuma special",
          "linkQuality": 0.91933405
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "fachberatung",
          "linkQuality": 0.9168273
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "mika – methoden im koffer für alle",
          "linkQuality": 0.9142499
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "wanderausstellung crossdressing",
          "linkQuality": 0.91160035
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "fuma fachtagung 2020",
          "linkQuality": 0.908877
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "#wissenslücken",
          "linkQuality": 0.9032032
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "kontakt",
          "linkQuality": 0.89721596
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "gender netzwerk nrw",
          "linkQuality": 0.8909032
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "mediathek",
          "linkQuality": 0.88762087
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": ["NOOPENER", "NOREFERRER"],
          "text": "saskia staible",
          "linkQuality": 0.8807971
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "",
          "linkQuality": 0.8698915
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "impressum",
          "linkQuality": 0.8175745
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "datenschutz",
          "linkQuality": 0.8125502
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "nutzungsbedingungen",
          "linkQuality": 0.80742013
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "agb",
          "linkQuality": 0.80218387
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "kontakt",
          "linkQuality": 0.7968411
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "mehr erfahren",
          "linkQuality": 0.74396247
        }
      }
    ],
    "externalLinks": [
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "newsletter",
          "linkQuality": 0.894101
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "fuma digital",
          "linkQuality": 0.88425267
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "zum newsletter anmelden",
          "linkQuality": 0.8273079
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "gefördert vom",
          "linkQuality": 0.7913915
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "IMG",
          "linkRels": [],
          "text": "",
          "linkQuality": 0.7801716
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "IMG",
          "linkRels": [],
          "text": "",
          "linkQuality": 0.7744014
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "IMG",
          "linkRels": [],
          "text": "",
          "linkQuality": 0.76852477
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": ["NOFOLLOW"],
          "text": "fuma",
          "linkQuality": 0.76254195
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "tremaze",
          "linkQuality": 0.7564535
        }
      },
      {
        "targetUrl": "[A CENSORED WEBSITE URL]",
        "linkInfo": {
          "linkType": "A",
          "linkRels": [],
          "text": "neu - designbüro",
          "linkQuality": 0.7502601
        }
      }
    ],
    "fetchStatus": "SUCCESS",
    "contentDigest": "K7+Al3O+GHiDmqgxgFKTfA\u003d\u003d",
    "semanticVector": "[0.0613852, 0.020461733, 0.08866751, 0.020461733, 0.0613852, 0.10230867, -0.0068205777, -0.047744043, 0.034102887, 0.0613852, 0.11594982, 0.11594982, 0.12959097, 0.034102887, 0.0613852, 0.0613852, 0.1841556, -0.075026356, -0.075026356, -0.08866751, -0.0068205777, 0.0068205777, 0.020461733, -0.0613852, -0.0068205777, -0.020461733, 0.034102887, -0.10230867, 0.034102887, -0.020461733, 0.0613852, -0.17051443, 0.020461733, -0.034102887, -0.020461733, 0.034102887, 0.0613852, -0.020461733, 0.12959097, -0.047744043, 0.020461733, -0.075026356, 0.12959097, 0.020461733, -0.0613852, -0.12959097, 0.0068205777, 0.047744043, 0.12959097, 0.047744043, -0.0613852, -0.10230867, 0.034102887, 0.047744043, 0.034102887, -0.0613852, 0.020461733, -0.020461733, 0.0068205777, 0.047744043, 0.020461733, 0.020461733, -0.17051443, 0.047744043, 0.0613852, -0.034102887, -0.10230867, 0.047744043, 0.0068205777, -0.0068205777, -0.21143791, -0.020461733, 0.08866751, -0.034102887, 0.0613852, -0.075026356, -0.075026356, 0.034102887, 0.11594982, 0.10230867, 0.075026356, 0.0068205777, -0.034102887, 0.020461733, 0.08866751, 0.12959097, 0.020461733, 0.0068205777, -0.020461733, 0.047744043, 0.020461733, -0.10230867, -0.047744043, 0.0613852, 0.075026356, -0.15687329, 0.020461733, -0.10230867, 0.0613852, 0.12959097, -0.0068205777, 0.020461733, 0.0068205777, -0.0613852, 0.0613852, 0.020461733, -0.020461733, -0.15687329, 0.0613852, 0.10230867, 0.034102887, 0.075026356, 0.0068205777, 0.020461733, 0.047744043, -0.10230867, -0.08866751, -0.0613852, -0.11594982, -0.047744043, 0.075026356, -0.075026356, 0.10230867, 0.10230867, 0.075026356, 0.034102887, -0.075026356, 0.020461733, -0.075026356, -0.075026356, -0.047744043, -0.0068205777, -0.047744043, -0.047744043, 0.034102887, 0.08866751, 0.21143791, 0.11594982, 0.020461733, 0.020461733, -0.020461733, 0.047744043, 0.075026356, 0.020461733, 0.0068205777, 0.11594982, -0.020461733, -0.0068205777, -0.0068205777, 0.0068205777, 0.020461733, 0.14323214, -0.10230867, 0.11594982, 0.08866751, 0.020461733, 0.11594982, -0.08866751, -0.034102887, -0.10230867, 0.020461733, 0.0613852, -0.075026356, 0.020461733, -0.047744043, -0.08866751, -0.0613852, -0.020461733, 0.020461733, -0.020461733, -0.047744043, -0.047744043, -0.075026356, -0.0068205777, 0.020461733, -0.020461733, 0.034102887, -0.020461733, 0.0068205777, -0.0068205777, 0.12959097, 0.0613852, 0.0613852, -0.034102887, -0.020461733, -0.08866751, 0.047744043, 0.08866751, 0.075026356, -0.0068205777, -0.0613852, 0.08866751]",
    "textSize": 384,
    "removedInternalLinks": [],
    "removedExternalLinks": [],
    "isFinalized": false,
    "ipAddress": "85.13.147.181",
    "metadata": [
      { "key": "apple-mobile-web-app-status-bar-style", "values": ["#6fdca3"] },
      { "key": "theme-color", "values": ["#6fdca3"] },
      { "key": "charset", "values": ["UTF-8"] },
      { "key": "msapplication-navbutton-color", "values": ["#6fdca3"] },
      {
        "key": "msapplication-tileimage",
        "values": [
          "[A CENSORED WEBSITE URL]"
        ]
      },
      {
        "key": "viewport",
        "values": ["width\u003ddevice-width, initial-scale\u003d1"]
      },
      {
        "key": "title",
        "values": [
          "[A CENSORED WEBSITE TITLE]"
        ]
      },
      {
        "key": "generator",
        "values": [
          "WordPress 5.4.2",
          "Powered by WPBakery Page Builder - drag and drop page builder for WordPress."
        ]
      }
    ],
    "fetchTimeToFirstByte": 1786
  },
  "history": { "fetchCount": 1, "changeCount": 0, "previousFetches": [] },
  "urlViewInfo": {
    "lastView": "2020-07-13 22:14",
    "lastSchedule": "2020-07-13 22:22",
    "lastFetch": "2020-07-13 22:23",
    "numInLinksExt": "1",
    "numInLinksInt": "0",
    "metrics": {
      "entries": [
        {
          "scope": "Page",
          "metrics": {
            "entries": [
              { "id": "ElementValue", "value": 1.3654977e-18 },
              { "id": "TrustValue", "value": 0.0013654961 }
            ]
          }
        }
      ]
    }
  },
  "url": "[A CENSORED WEBSITE URL]"
}
```