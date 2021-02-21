# Crawling baby web pages

## Data heads
Examples of what the data looks like:

```
{"fetch":{"depth":0,"language":"de","textQuality":1.497677,"contentLength":52686,"httpStatus":200,"title":"Rassismuskritische Perspektiven auf häusliche Gewalt – FUMA Fachstelle Gender \u0026 Diversität","fetchDuration":2058,"fetchDate":"2020-07-13 22:22","parsingErrors":0,"robotsTag":{"NOINDEX":false,"NOFOLLOW":false,"NOARCHIVE":false,"NOSNIPPET":false},"internalLinks":[{"targetUrl":"https://www.gender-nrw.de/?p\u003d2811","linkInfo":{"linkType":"LINK","linkRels":["SHORTLINK"],"text":"","linkQuality":0.99308187}},{"targetUrl":"https://www.gender-nrw.de/feed/","linkInfo":{"linkType":"LINK","linkRels":["ALTERNATE"],"text":"","linkQuality":0.99183744}},{"targetUrl":"https://www.gender-nrw.de/comments/feed/","linkInfo":{"linkType":"LINK","linkRels":["ALTERNATE"],"text":"","linkQuality":0.9915631}},{"targetUrl":"https://www.gender-nrw.de/wp-json/oembed/1.0/embed?url\u003dhttps%3A%2F%2Fwww.gender-nrw.de%2Fhaeusliche-gewalt%2F","linkInfo":{"linkType":"LINK","linkRels":["ALTERNATE"],"text":"","linkQuality":0.97252625}},{"targetUrl":"https://www.gender-nrw.de/wp-json/oembed/1.0/embed?url\u003dhttps%3A%2F%2Fwww.gender-nrw.de%2Fhaeusliche-gewalt%2F\u0026format\u003dxml","linkInfo":{"linkType":"LINK","linkRels":["ALTERNATE"],"text":"","linkQuality":0.97162145}},{"targetUrl":"https://www.gender-nrw.de/","linkInfo":{"linkType":"IMG","linkRels":[],"text":"fuma fachstelle gender \u0026 diversität","linkQuality":0.96770453}},{"targetUrl":"https://www.gender-nrw.de/unsere-vision/","linkInfo":{"linkType":"A","linkRels":[],"text":"unsere vision","linkQuality":0.96555483}},{"targetUrl":"https://www.gender-nrw.de/imagefilm/","linkInfo":{"linkType":"A","linkRels":[],"text":"unser imagefilm","linkQuality":0.9644288}},{"targetUrl":"https://www.gender-nrw.de/geschlechtergerechte-sprache/","linkInfo":{"linkType":"A","linkRels":[],"text":"geschlechtergerechte sprache","linkQuality":0.9632674}},{"targetUrl":"https://www.gender-nrw.de/stellenangebote/","linkInfo":{"linkType":"A","linkRels":[],"text":"stellenangebote","linkQuality":0.9620696}},{"targetUrl":"https://www.gender-nrw.de/fuma-e-v/","linkInfo":{"linkType":"A","linkRels":[],"text":"fuma e.v.","linkQuality":0.96083426}},{"targetUrl":"https://www.gender-nrw.de/team/","linkInfo":{"linkType":"A","linkRels":[],"text":"team","linkQuality":0.95956045}},{"targetUrl":"https://www.gender-nrw.de/home_new/fuma-fortbildungen/","linkInfo":{"linkType":"A","linkRels":[],"text":"fuma fortbildungen","linkQuality":0.9568927}},{"targetUrl":"https://www.gender-nrw.de/home_new/fuma-fortbildungen/rassismuskritikkritischesweisssein/","linkInfo":{"linkType":"A","linkRels":[],"text":"rassismuskritik \u0026 kritisches weißsein","linkQuality":0.9540575}},{"targetUrl":"https://www.gender-nrw.de/home_new/fuma-fortbildungen/rassismus-maennlichekeiten/","linkInfo":{"linkType":"A","linkRels":[],"text":"rassismus, männlichkeiten*, empowerment","linkQuality":0.95257413}},{"targetUrl":"https://www.gender-nrw.de/home_new/fuma-fortbildungen/der-anti-bias-ansatz/","linkInfo":{"linkType":"A","linkRels":[],"text":"der anti-bias-ansatz","linkQuality":0.95104533}},{"targetUrl":"https://www.gender-nrw.de/home_new/fuma-fortbildungen/discover-diversity-island/","linkInfo":{"linkType":"A","linkRels":[],"text":"discover diversity island","linkQuality":0.94946986}},{"targetUrl":"https://www.gender-nrw.de/home_new/fuma-fortbildungen/bodyismuskritische-paedagogik/","linkInfo":{"linkType":"A","linkRels":[],"text":"bodyismuskritische pädagogik","linkQuality":0.94617385}},{"targetUrl":"https://www.gender-nrw.de/digitale-lernwelten/connect-qualifizierungsreihe/","linkInfo":{"linkType":"A","linkRels":[],"text":"#connect qualifizierungsreihe","linkQuality":0.94445074}},{"targetUrl":"https://www.gender-nrw.de/digitale-lernwelten/","linkInfo":{"linkType":"A","linkRels":[],"text":"digitale lernwelten","linkQuality":0.9426758}},{"targetUrl":"https://www.gender-nrw.de/digitale-lernwelten/fuma-talks-fuma-web-seminare/","linkInfo":{"linkType":"A","linkRels":[],"text":"fuma – talks \u0026 web-seminare","linkQuality":0.94084775}},{"targetUrl":"https://www.gender-nrw.de/colorism-2/","linkInfo":{"linkType":"A","linkRels":[],"text":"colorism","linkQuality":0.93896514}},{"targetUrl":"https://www.gender-nrw.de/unsagbar-undenkbar/","linkInfo":{"linkType":"A","linkRels":[],"text":"sexualisierte gewalt durch kolleg_innen","linkQuality":0.9370266}},{"targetUrl":"https://www.gender-nrw.de/digitale-lernwelten/lernkarten/","linkInfo":{"linkType":"A","linkRels":[],"text":"fuma lernkarten","linkQuality":0.9350308}},{"targetUrl":"https://www.gender-nrw.de/fuma_friends-2/","linkInfo":{"linkType":"A","linkRels":[],"text":"fuma \u0026 friends","linkQuality":0.9329763}},{"targetUrl":"https://www.gender-nrw.de/digitale-lernwelten/selbstlernkurs-jump-in/","linkInfo":{"linkType":"A","linkRels":[],"text":"selbstlernkurs ‘jump in’","linkQuality":0.9308616}},{"targetUrl":"https://www.gender-nrw.de/bit/","linkInfo":{"linkType":"A","linkRels":[],"text":"#bit* basics inter* und trans*","linkQuality":0.92868525}},{"targetUrl":"https://www.gender-nrw.de/digitale-lernwelten/fuma-erklaervideos/","linkInfo":{"linkType":"A","linkRels":[],"text":"fuma-erklärvideos","linkQuality":0.92644584}},{"targetUrl":"https://www.gender-nrw.de/digitale-lernwelten/connect-community/","linkInfo":{"linkType":"A","linkRels":[],"text":"#connect community","linkQuality":0.9241418}},{"targetUrl":"https://www.gender-nrw.de/home_new/inhouse-on-demand/","linkInfo":{"linkType":"A","linkRels":[],"text":"inhouse / on demand","linkQuality":0.92177176}},{"targetUrl":"https://www.gender-nrw.de/specials/","linkInfo":{"linkType":"A","linkRels":[],"text":"fuma special","linkQuality":0.91933405}},{"targetUrl":"https://www.gender-nrw.de/fachberatung/","linkInfo":{"linkType":"A","linkRels":[],"text":"fachberatung","linkQuality":0.9168273}},{"targetUrl":"https://www.gender-nrw.de/mika-methoden-im-koffer-fuer-alle-vorurteilsreflektierte-paedagogik/","linkInfo":{"linkType":"A","linkRels":[],"text":"mika – methoden im koffer für alle","linkQuality":0.9142499}},{"targetUrl":"https://www.gender-nrw.de/wanderausstellung-cross-dressing/","linkInfo":{"linkType":"A","linkRels":[],"text":"wanderausstellung crossdressing","linkQuality":0.91160035}},{"targetUrl":"https://www.gender-nrw.de/fachtagung2020/","linkInfo":{"linkType":"A","linkRels":[],"text":"fuma fachtagung 2020","linkQuality":0.908877}},{"targetUrl":"https://www.gender-nrw.de/wissensluecken/","linkInfo":{"linkType":"A","linkRels":[],"text":"#wissenslücken","linkQuality":0.9032032}},{"targetUrl":"https://www.gender-nrw.de/contact/","linkInfo":{"linkType":"A","linkRels":[],"text":"kontakt","linkQuality":0.89721596}},{"targetUrl":"https://www.gender-nrw.de/gendernetzwerk-nrw/","linkInfo":{"linkType":"A","linkRels":[],"text":"gender netzwerk nrw","linkQuality":0.8909032}},{"targetUrl":"https://www.gender-nrw.de/mediathek/","linkInfo":{"linkType":"A","linkRels":[],"text":"mediathek","linkQuality":0.88762087}},{"targetUrl":"https://www.gender-nrw.de/saskia-staible/","linkInfo":{"linkType":"A","linkRels":["NOOPENER","NOREFERRER"],"text":"saskia staible","linkQuality":0.8807971}},{"targetUrl":"https://www.gender-nrw.de/digitale_jugend/","linkInfo":{"linkType":"A","linkRels":[],"text":"","linkQuality":0.8698915}},{"targetUrl":"https://www.gender-nrw.de/impressum","linkInfo":{"linkType":"A","linkRels":[],"text":"impressum","linkQuality":0.8175745}},{"targetUrl":"https://www.gender-nrw.de/datenschutz","linkInfo":{"linkType":"A","linkRels":[],"text":"datenschutz","linkQuality":0.8125502}},{"targetUrl":"https://www.gender-nrw.de/nutzungsbedingungen","linkInfo":{"linkType":"A","linkRels":[],"text":"nutzungsbedingungen","linkQuality":0.80742013}},{"targetUrl":"https://www.gender-nrw.de/agb","linkInfo":{"linkType":"A","linkRels":[],"text":"agb","linkQuality":0.80218387}},{"targetUrl":"https://www.gender-nrw.de/contact","linkInfo":{"linkType":"A","linkRels":[],"text":"kontakt","linkQuality":0.7968411}},{"targetUrl":"https://www.gender-nrw.de/datenschutz/","linkInfo":{"linkType":"A","linkRels":[],"text":"mehr erfahren","linkQuality":0.74396247}}],"externalLinks":[{"targetUrl":"http://gender-nrw.de/newsletter","linkInfo":{"linkType":"A","linkRels":[],"text":"newsletter","linkQuality":0.894101}},{"targetUrl":"https://fumadigital.de/","linkInfo":{"linkType":"A","linkRels":[],"text":"fuma digital","linkQuality":0.88425267}},{"targetUrl":"https://subscribe.newsletter2go.com/?n2g\u003dfbmcmc4n-ybahng8j-12fo","linkInfo":{"linkType":"A","linkRels":[],"text":"zum newsletter anmelden","linkQuality":0.8273079}},{"targetUrl":"https://www.mkffi.nrw/","linkInfo":{"linkType":"A","linkRels":[],"text":"gefördert vom","linkQuality":0.7913915}},{"targetUrl":"https://www.facebook.com/FUMAFachstellegender/","linkInfo":{"linkType":"IMG","linkRels":[],"text":"","linkQuality":0.7801716}},{"targetUrl":"https://www.instagram.com/fuma_fachstelle/","linkInfo":{"linkType":"IMG","linkRels":[],"text":"","linkQuality":0.7744014}},{"targetUrl":"https://www.youtube.com/user/FUMAessen","linkInfo":{"linkType":"IMG","linkRels":[],"text":"","linkQuality":0.76852477}},{"targetUrl":"http://gender-nrw.de/","linkInfo":{"linkType":"A","linkRels":["NOFOLLOW"],"text":"fuma","linkQuality":0.76254195}},{"targetUrl":"https://tremaze.de/","linkInfo":{"linkType":"A","linkRels":[],"text":"tremaze","linkQuality":0.7564535}},{"targetUrl":"http://www.neu-designbuero.de/","linkInfo":{"linkType":"A","linkRels":[],"text":"neu - designbüro","linkQuality":0.7502601}}],"fetchStatus":"SUCCESS","contentDigest":"K7+Al3O+GHiDmqgxgFKTfA\u003d\u003d","semanticVector":"[0.0613852, 0.020461733, 0.08866751, 0.020461733, 0.0613852, 0.10230867, -0.0068205777, -0.047744043, 0.034102887, 0.0613852, 0.11594982, 0.11594982, 0.12959097, 0.034102887, 0.0613852, 0.0613852, 0.1841556, -0.075026356, -0.075026356, -0.08866751, -0.0068205777, 0.0068205777, 0.020461733, -0.0613852, -0.0068205777, -0.020461733, 0.034102887, -0.10230867, 0.034102887, -0.020461733, 0.0613852, -0.17051443, 0.020461733, -0.034102887, -0.020461733, 0.034102887, 0.0613852, -0.020461733, 0.12959097, -0.047744043, 0.020461733, -0.075026356, 0.12959097, 0.020461733, -0.0613852, -0.12959097, 0.0068205777, 0.047744043, 0.12959097, 0.047744043, -0.0613852, -0.10230867, 0.034102887, 0.047744043, 0.034102887, -0.0613852, 0.020461733, -0.020461733, 0.0068205777, 0.047744043, 0.020461733, 0.020461733, -0.17051443, 0.047744043, 0.0613852, -0.034102887, -0.10230867, 0.047744043, 0.0068205777, -0.0068205777, -0.21143791, -0.020461733, 0.08866751, -0.034102887, 0.0613852, -0.075026356, -0.075026356, 0.034102887, 0.11594982, 0.10230867, 0.075026356, 0.0068205777, -0.034102887, 0.020461733, 0.08866751, 0.12959097, 0.020461733, 0.0068205777, -0.020461733, 0.047744043, 0.020461733, -0.10230867, -0.047744043, 0.0613852, 0.075026356, -0.15687329, 0.020461733, -0.10230867, 0.0613852, 0.12959097, -0.0068205777, 0.020461733, 0.0068205777, -0.0613852, 0.0613852, 0.020461733, -0.020461733, -0.15687329, 0.0613852, 0.10230867, 0.034102887, 0.075026356, 0.0068205777, 0.020461733, 0.047744043, -0.10230867, -0.08866751, -0.0613852, -0.11594982, -0.047744043, 0.075026356, -0.075026356, 0.10230867, 0.10230867, 0.075026356, 0.034102887, -0.075026356, 0.020461733, -0.075026356, -0.075026356, -0.047744043, -0.0068205777, -0.047744043, -0.047744043, 0.034102887, 0.08866751, 0.21143791, 0.11594982, 0.020461733, 0.020461733, -0.020461733, 0.047744043, 0.075026356, 0.020461733, 0.0068205777, 0.11594982, -0.020461733, -0.0068205777, -0.0068205777, 0.0068205777, 0.020461733, 0.14323214, -0.10230867, 0.11594982, 0.08866751, 0.020461733, 0.11594982, -0.08866751, -0.034102887, -0.10230867, 0.020461733, 0.0613852, -0.075026356, 0.020461733, -0.047744043, -0.08866751, -0.0613852, -0.020461733, 0.020461733, -0.020461733, -0.047744043, -0.047744043, -0.075026356, -0.0068205777, 0.020461733, -0.020461733, 0.034102887, -0.020461733, 0.0068205777, -0.0068205777, 0.12959097, 0.0613852, 0.0613852, -0.034102887, -0.020461733, -0.08866751, 0.047744043, 0.08866751, 0.075026356, -0.0068205777, -0.0613852, 0.08866751]","textSize":384,"removedInternalLinks":[],"removedExternalLinks":[],"isFinalized":false,"ipAddress":"85.13.147.181","metadata":[{"key":"apple-mobile-web-app-status-bar-style","values":["#6fdca3"]},{"key":"theme-color","values":["#6fdca3"]},{"key":"charset","values":["UTF-8"]},{"key":"msapplication-navbutton-color","values":["#6fdca3"]},{"key":"msapplication-tileimage","values":["https://www.gender-nrw.de/wp-content/uploads/2018/04/cropped-favicon-270x270.png"]},{"key":"viewport","values":["width\u003ddevice-width, initial-scale\u003d1"]},{"key":"title","values":["Rassismuskritische Perspektiven auf häusliche Gewalt – FUMA Fachstelle Gender \u0026 Diversität"]},{"key":"generator","values":["WordPress 5.4.2","Powered by WPBakery Page Builder - drag and drop page builder for WordPress."]}],"fetchTimeToFirstByte":1786},"history":{"fetchCount":1,"changeCount":0,"previousFetches":[]},"urlViewInfo":{"lastView":"2020-07-13 22:14","lastSchedule":"2020-07-13 22:22","lastFetch":"2020-07-13 22:23","numInLinksExt":"1","numInLinksInt":"0","metrics":{"entries":[{"scope":"Page","metrics":{"entries":[{"id":"ElementValue","value":1.3654977E-18},{"id":"TrustValue","value":0.0013654961}]}}]}},"url":"https://www.gender-nrw.de/haeusliche-gewalt/"}
```