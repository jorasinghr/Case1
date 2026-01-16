# Case1
## Caseoppgave
### Scenario
En fiktiv kunde som leverer leie av sykler rundt om i Oslo by har laget endepunkter for sine data. De ønsker nå å begynne å ta datadrevne beslutninger for hvor de skal plassere nye stasjoner. I den forstand har de nå etterspurt at du hjelper de med å tilgjengeliggjøre dataene til deres analytikere og stakeholdere. Din oppgave er å levere en POC data pipeline for å hjelpe de ta datadrevne beslutninger.

### Forretningsmålet
Målet er å lage brukbare dataprodukter som data analytikere kan bruke til å vise til interessenter. Derfor spør de om å få tilgjengeliggjort ren og pen data fra endepunktene de har hørt om, i et vedvarende og spørrbart format (persistent og query-able). De ønsker også en metrikk på hvor mye brukt stasjonene er, hva nå enn det betyr er ikke spesifisert og blir opp til deg å bestemme. Om det er noe annet kult du ønsker å gjøre med dataene så er det fritt fram. Hvor mye eller lite du gjør ut av oppgaven er opp til deg.

### Dataene
Dataen du skal bruke ligger på REST-endepunkter her.

API Base-URL: https://oslo-bike-api-13322556367.europe-west1.run.app

GET-Endepunkter:
- [station_information] : Dette endepunktet inneholder metadata om sykkelstasjonene. Dette inkluderer ID, navn, adresse, posisjon og kapasitet.
- [station_status] : Dette endepunktet inneholder data om statusen til sykkelstasjonene akkurat nå.
- [pass_sales] : Dette punktet inneholder data på salg av tilganger, både månedlige, daglige og enkelt-pass.

### Leveranse
Vi håper du har anledning til å levere løsningen i en zip-fil over mail eller en link til et repo. Løsningen bør inneholde følgende:
1. Python-filen(e) som svarer på oppgaven.
2. en requirements.txt som lister nødvendige pakker så vi får kjørt det.
3. en README.md som beskriver hvordan man setter opp og kjører koden, samt en kort beskrivelse av løsningen og designvalgene som ble tatt.

## Løsning
### How-to
Koden kjøres fra notebooks(.ipynb-filene) som avsluttes med _pipeline. Det er lagt opp til at man kjører ting lokalt uten behov for database eller sky-løsning. Notebooks kan kjøres så ofte man vil, men det genereres filer for hver kjøring. 
Jeg har kjørt notebooks i VScode og jupyter notebook, men annen løsning funker fint. 

### Designvalg
Løsningen ble designet etter medaljong-arkitektur prinsippene og har derfor bronze/silver/gold layers. Siden dette er en POC har mye blitt forenklet, hovedsakelig pga arbeidsmengde. Det er en lakehouse som ligger til grunn for datalagring med strukturen data/<'egendefinert navn'>/<'egendefinert navn'>_<timestamp>.parquet. Dataene kan hentes ut på alle måter som støtter parquet filer evt. lastes inn i en database (hvis det er ønskelig).

### arkitektur
Det er en stor fil som inneholder de fleste funksjoner som brukes. Denne ligger under utils/functions.py. Dette er en forenkling og kan deles opp etter lag, datakilde eller annet fornuftig fordeling. Denne filen inneholder alt av innhenting av data til innlast til bronze/silver/gold, og fremvisning av siste oppdaterte versjon. Det ligger også egne funksjoner i _pipelines filene som er spesifiserte for den enkelte pipelinen.
