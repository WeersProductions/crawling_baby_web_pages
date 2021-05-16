## analysis_pipeline.py
This file should be running from a node on the cluster. It will call the various spark commands to process from the 'raw' data to a form with labeled values.

The file itself contains an explanation on how to run it.

### Resulting schema
```
root
 |-- url: string (nullable = true)
 |-- fetch_contentDigest: string (nullable = true)
 |-- fetch_contentLength: long (nullable = true)
 |-- fetch_textSize: long (nullable = true)
 |-- fetch_textQuality: double (nullable = true)
 |-- fetch_semanticVector: string (nullable = true)
 |-- fetchMon: integer (nullable = true)
 |-- fetchDay: integer (nullable = true)
 |-- n_internalInLinks: integer (nullable = true)
 |-- n_externalInLinks: integer (nullable = true)
 |-- n_internalOutLinks: integer (nullable = true)
 |-- n_externalOutLinks: integer (nullable = true)
 |-- internalOutLinks: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- linkInfo: struct (nullable = true)
 |    |    |    |-- linkQuality: double (nullable = true)
 |    |    |    |-- linkRels: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- linkType: string (nullable = true)
 |    |    |    |-- text: string (nullable = true)
 |    |    |-- targetUrl: string (nullable = true)
 |-- externalOutLinks: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- linkInfo: struct (nullable = true)
 |    |    |    |-- linkQuality: double (nullable = true)
 |    |    |    |-- linkRels: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- linkType: string (nullable = true)
 |    |    |    |-- text: string (nullable = true)
 |    |    |-- targetUrl: string (nullable = true)
 |-- history_changeCount: long (nullable = true)
 |-- history_fetchCount: long (nullable = true)
 |-- finalInternalOutLinks: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- finalExternalOutLinks: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- diffExternalOutLinks: integer (nullable = true)
 |-- diffInternalOutLinks: integer (nullable = true)
```