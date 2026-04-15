**Project Data Engineering - Casus**

**Aanleiding**

Jouw team is ingezet om een opdracht te doen voor _Great Outdoors_: een groothandel van allerlei materialen die te maken hebben met het buitenleven. Je kan hierbij denken aan campingspullen, buitenkleding, vishengels en insectenspray. Great Outdoors heeft de laatste jaren opgemerkt dat haar concurrenten steeds vaker op een _datagedreven manier_ werken. Beslissingen worden hierbij niet langer genomen op basis van onderbuikgevoel en/of subjectiviteit, maar op basis van objectief meet-/verantwoordbare data. Zo kunnen deze concurrenten dus steeds sneller de meest effectieve beslissingen formuleren en doorvoeren. Great Outdoors past deze manier van werken zelf nog niet toe, waardoor zij onderhand vreest dat op termijn haar marktaandeel in gevaar komt.

**Probleemstellingen**

De bovengenoemde inzichten waren dusdanig nuttig dat bij Great Outdoors de behoefte is ontstaan om ook de eigen bedrijfsvoering in kaart te brengen en te verbeteren met behulp van al haar verzamelde gegevens. De interne datahuishouding is deels al in kaart gebracht in een aantal modellen, maar deze zijn nog niet compleet en hebben ook nog niet geresulteerd in werkende data-analyseproducten. Tijdens een brainstorm is er een aantal problemen naar voren gekomen die wellicht zouden kunnen worden opgelost met behulp van dergelijke producten.

De lijst met knelpunten is als volgt:

*   Knelpunt 1: Er is momenteel onvoldoende inzicht in het bestelgedrag van klanten. Men weet niet goed welke producten in welke periodes, regio’s of klantsegmenten het meest worden besteld. Ook is onduidelijk welke combinaties van producten vaak samen worden besteld en welke klanten verantwoordelijk zijn voor het grootste deel van de omzet. Hierdoor is het lastig om gerichte commerciële beslissingen te nemen. Dit wordt gezien als het meest urgente knelpunt, aangezien Great Outdoors niet kan blijven voortbestaan als ze weinig tot niks verkoopt.
*   Knelpunt 2: Er is weinig inzicht in retourstromen. Het is onduidelijk welke producten relatief vaak worden teruggebracht, in welke regio’s dit gebeurt en wat de onderliggende redenen zijn (bijvoorbeeld defect, verkeerde levering of ontevredenheid). Hierdoor blijven structurele kwaliteits- of logistieke problemen mogelijk onopgemerkt.
*   Knelpunt 3: Er is nagenoeg geen inzicht in hoe tevreden klanten eigenlijk zijn met aangekochte producten. Daarom heeft men het gevoel dat het assortiment al jarenlang hetzelfde is, zonder dat er tussentijds geëvalueerd wordt of deze nog toereikend is van wat klanten willen. Reviewscores uit de data kunnen o.a. een aanknopingspunt bieden.
*   Knelpunt 4: Great Outdoors investeert in cursussen voor medewerkers en partners, maar heeft geen duidelijk beeld van het effect hiervan. Er is geen inzicht in wie welke cursus volgt, wat de kosten per cursus zijn en of deelname leidt tot betere verkoopresultaten, minder fouten of hogere klanttevredenheid. Hierdoor is het moeilijk om te bepalen welke cursussen daadwerkelijk waarde toevoegen.
*   Knelpunt 5: Men heeft het gevoel dat meer dan de helft van wat wordt ingekocht niet wordt verkocht aan groothandels, maar ongebruikt in de eigen magazijnen blijft liggen. Onderzocht dient te worden of dit het geval is en zo ja: de oorzaken hiervan
*   Knelpunt 6: Er worden jaarlijks verkoopdoelen vastgesteld per regio, productcategorie en accountmanager, maar het ontbreekt aan een gestructureerd overzicht waarin doelstellingen en gerealiseerde verkoop systematisch met elkaar worden vergeleken. Hierdoor is het lastig om tijdig bij te sturen of prestaties objectief te evalueren. 
*   Knelpunt 7: Er worden wel verkoopverwachtingen opgesteld, maar het is onduidelijk hoe accuraat deze voorspellingen zijn. Er is geen historisch overzicht waarin verwachtingen en daadwerkelijke verkoop naast elkaar worden gezet. Hierdoor kan men de betrouwbaarheid van prognoses niet beoordelen en worden productie- en inkoopbeslissingen mogelijk op basis van onnauwkeurige verwachtingen genomen.

Belangrijk: bij Great Outdoors staat transparantie hoog in het vaandel. Zij heeft expliciet de wens uitgesproken dat iedereen, ongeacht de mate van technische IT-kennis, elk van de bovenstaande analyses moet kunnen uitvoeren. Wél is zij bereid om iedereen een Power-BI-cursus te geven, daarmee is het geen probleem om dashboards in deze tool aan te leveren.

**Opdracht**

Elk projectgroepje gaat 3 van deze problemen oplossen met behulp van de geleerde theorie/technieken over Source Data Modelling, Datawarehousing & Dashboarding. Iedereen gaat met knelpunt 1 aan de slag, de overige 2 knelpunten mogen per groep zelf uitgekozen worden. Wél dient er binnen een klas voldoende variatie hierin te zijn, dit zal worden gecontroleerd door de DE-docenten. Er wordt verwacht dat elke projectgroep uiteindelijk een review/demo kan geven van de volledige datapijplijn, inclusief (modellen in) documentatie.

**Data & reeds gemaakte ontwerpen**

Alle data en reeds gemaakte ontwerpen vind je [hier](/d2l/common/viewFile.d2lfile/Content/L2NvbnRlbnQvZW5mb3JjZWQvMTY0ODE2LUgtSUNULVNFNC0yNV8yMDI1X1NQUklOR18yL0dyZWF0T3V0ZG9vcnMuemlw/GreatOutdoors.zip?ou=164816). Hier volgt een korte beschrijving hiervan:

*   GO\_SALES-data: een database die alle feiten en dimensies over datgene wat voor organisaties essentieel is: bestelling en verkopen.
*   CRM-data: een database die nadere details bevat over klanten. 
*   GO\_STAFF-data: een database die nadere details bevat over medewerkers.
*   INVENTORY\_LEVELS-data: een CSV-bestand dat de voorraadniveaus per product per maand weergeeft.
*   PRODUCT\_FORECAST-data: een CSV-bestand dat de verwachte verkopen per product per maand weergeeft.
*   SALES\_TARGET-data: een CSV-bestand dat de verkoopdoelen per product per maand weergeeft. Dit is niet hetzelfde als de bovengenoemde verkoopverwachtingen.
