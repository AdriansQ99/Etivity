from multiprocessing import synchronize
from mysqlx import Session
from sqlalchemy import INTEGER, VARCHAR, Boolean, Date, DateTime, Time, ForeignKey, Column, Integer, MetaData, delete, null, select, true, update
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

engine = create_engine("mysql+mysqlconnector://root:*******@localhost/Etivity", echo = True)

Session = sessionmaker(bind = engine)
session = Session()

# Dichiarazione tabelle create in MySQL
class Persone(Base):
    __tablename__ = "Persone"
    id_persona = Column(Integer, primary_key = True)
    nome = Column(VARCHAR(30))
    cognome = Column(VARCHAR(30))
    luogonascita = Column(VARCHAR(30))
    datanascita = Column(Date)
    nazionalità = Column(VARCHAR(30))

class Discipline(Base):
    __tablename__ = "Discipline"
    id_disciplina = Column(Integer, primary_key = True)
    nomed = Column(VARCHAR(255))

class Artisti(Base):
    __tablename__ = "Artisti"
    id_artista = Column(Integer, primary_key = True)
    datamorte = Column(Date)
    id_disciplina = Column(Integer, ForeignKey("Discipline.id_disciplina"))
    id_persona = Column(Integer, ForeignKey("Persone.id_persona"))

class TitoloStudio(Base):
    __tablename__ = "TitoloStudio"
    codicetitolo = Column(Integer, primary_key = True)
    descrizione = Column(VARCHAR(255))

class Musei(Base):
    __tablename__ = "Musei"
    codicemuseo = Column(Integer, primary_key = True)
    nomemuseo = Column(VARCHAR(50))
    indirizzo = Column(VARCHAR(100))
    telefono = Column(VARCHAR(16))
    costobiglietto = Column(Integer)
    stabilità = Column(Boolean, default=True)
    periodoapertura = Column(VARCHAR(100))

class Aperture(Base):
    __tablename__ = "Aperture"
    id_apertura = Column(Integer, primary_key = True)
    giorno = Column(VARCHAR(9))
    oraapertura = Column(Time)
    orachiusura = Column(Time)
    codicemuseo = Column(Integer, ForeignKey("Musei.codicemuseo"))

class Dipendenti(Base):
    __tablename__ = "Dipendenti"
    id_dipendente = Column(Integer, primary_key = True)
    mansione = Column(VARCHAR(30))
    dataassunzione = Column(Date)
    cittàresidenza = Column(VARCHAR(30))
    indirizzoresidenza = Column(VARCHAR(100))
    cf = Column(VARCHAR(16))
    codicetitolo = Column(Integer, ForeignKey("TitoloStudio.codicetitolo"))
    codicemuseo = Column(Integer, ForeignKey("Musei.codicemuseo"))
    id_persona = Column(Integer, ForeignKey("Persone.id_persona"))

class Collaboratori(Base):
    __tablename__ = "Collaboratori"
    id_collaboratore = Column(INTEGER, primary_key = True)
    mansione = Column(VARCHAR(30))
    cittàresidenza = Column(VARCHAR(30))
    indirizzoresidenza = Column(VARCHAR(100))
    cf = Column(VARCHAR(16))
    codicetitolo = Column(Integer, ForeignKey("TitoloStudio.codicetitolo"))
    codicemuseo = Column(Integer, ForeignKey("Musei.codicemuseo"))
    id_persona = Column(Integer, ForeignKey("Persone.id_persona"))
  
class Direttori(Base):
    __tablename__ = "Direttori"
    id_direttore = Column(Integer, primary_key = True)
    dataassunzione = Column(Date)
    datanomina = Column(Date)
    cittàresidenza = Column(VARCHAR(30))
    indirizzoresidenza = Column(VARCHAR(100))
    cf = Column(VARCHAR(16))
    codicetitolo = Column(Integer, ForeignKey("TitoloStudio.codicetitolo"))
    codicemuseo = Column(Integer, ForeignKey("Musei.codicemuseo"))
    id_persona = Column(Integer, ForeignKey("Persone.id_persona"))

class CorrentiArtistiche(Base):
    __tablename__ = "CorrentiArtistiche"
    id_corrente = Column(Integer, primary_key = True)
    nomeca = Column(VARCHAR(30))
    datainizio = Column(VARCHAR(100))
    datafine = Column(VARCHAR(100))
    influenze = Column(VARCHAR(255))
    codicemuseo = Column(Integer, ForeignKey("Musei.codicemuseo"))
    id_artista = Column(Integer, ForeignKey("Artisti.id_artista"))

class TipologieOpere(Base):
    __tablename__ = "TipologieOpere"
    codice = Column(Integer, primary_key = True)
    nometipologia = Column(VARCHAR(100))
    tecnica = Column(VARCHAR(255))

class Opere(Base):
    __tablename__ = "Opere"
    id_opera = Column(Integer, primary_key = True)
    datarealizzazione = Column(INTEGER)
    titoloopera = Column(VARCHAR(100))
    disponibilità = Column(Boolean, default=True)
    datarientro = Column(Date)
    codicemuseo = Column(Integer, ForeignKey("Musei.codicemuseo"))
    id_artista = Column(Integer, ForeignKey("Artisti.id_artista"))
    codice = Column(Integer, ForeignKey("TipologieOpere.codice"))

class DescrizioniArtistiche(Base):
    __tablename__ = "DescrizioniArtistiche"
    id_desart = Column(Integer, primary_key = True)
    descrizionea = Column(VARCHAR(255))
    id_opera = Column(Integer, ForeignKey("Opere.id_opera"))

class DescrizioniStoriche(Base):
    __tablename__ = "DescrizioniStoriche"
    id_desstor = Column(Integer, primary_key = True)
    descriziones = Column(VARCHAR(255))
    id_opera = Column(Integer, ForeignKey("Opere.id_opera"))

# Inserimento dati in tabelle
p1 = Persone(id_persona = 1, nome = "Leonardo", cognome = "Da Vinci", luogonascita = "Anchiano", datanascita = "1452-04-15", nazionalità = "Italiana")
p2 = Persone(id_persona = 2, nome = "Michelangelo", cognome = "Buonarroti", luogonascita = "Caprese Michelangelo", datanascita = "1475-03-06", nazionalità = "Italiana")
p3 = Persone(id_persona = 3, nome = "Mario", cognome = "Verdi", luogonascita = "Ancona", datanascita = "1970-11-11", nazionalità = "Italiana")
p4 = Persone(id_persona = 4, nome = "Antonio", cognome = "Decimi", luogonascita = "Novara", datanascita = "1983-08-27", nazionalità = "Italiana")
p5 = Persone(id_persona = 5, nome = "Nadia", cognome = "Nascimento", luogonascita = "Valencia", datanascita = "1989-03-18", nazionalità = "Spagnola")
p6 = Persone(id_persona = 6, nome = "Tiziano", cognome = "Vecellio", luogonascita = "Pieve di Cadore", datanascita = "1488-01-12", nazionalità = "Italiana")
p7 = Persone(id_persona = 7, nome = "Jhin", cognome = "Nagato", luogonascita = "Kyoto", datanascita = "1963-05-18", nazionalità = "Giapponese")
p8 = Persone(id_persona = 8, nome = "Giovanni", cognome = "Bellini", luogonascita = "Venezia", datanascita = "1427-11-20", nazionalità = "Italiana")
p9 = Persone(id_persona = 9, nome = "Francesca", cognome = "Nardi", luogonascita = "Roma", datanascita = "1987-12-04", nazionalità = "Italiana")
p10 = Persone(id_persona = 10, nome = "Frank", cognome = "Steward", luogonascita = "Atlanta", datanascita = "1985-08-15", nazionalità = "Statunitense")
session.add_all([p1, p2, p3, p4, p5, p6, p7, p8, p9, p10])
session.commit()

disc1 = Discipline(id_disciplina = 1, nomed = "Pittore")
disc2 = Discipline(id_disciplina = 2, nomed = "Scultore")
disc3 = Discipline(id_disciplina = 3, nomed = "Architetto")
session.add_all([disc1, disc2, disc3])
session.commit()

art1 = Artisti(id_artista = 3, datamorte = "1576-08-27", id_disciplina = 1, id_persona = 6)
art2 = Artisti(id_artista = 4, datamorte = "1516-11-29", id_disciplina = 1, id_persona = 8)
session.add_all([art1, art2])
session.commit()

ts1 = TitoloStudio(codicetitolo = 1, descrizione = "Licenza elementare")
ts2 = TitoloStudio(codicetitolo = 2, descrizione = "Licenza media")
ts3 = TitoloStudio(codicetitolo = 3, descrizione = "Licenza superiore")
ts4 = TitoloStudio(codicetitolo = 4, descrizione = "Laurea triennale")
ts5 = TitoloStudio(codicetitolo = 5, descrizione = "Laurea magistrale")
session.add_all([ts1, ts2, ts3, ts4, ts5])
session.commit()

mus1 = Musei(codicemuseo = 33, nomemuseo = "Museo arte medioevale e moderna", indirizzo = "Piazza Emeritani, 8, Padova", telefono = "0498204551", costobiglietto = "10")
mus2 = Musei(codicemuseo = 34, nomemuseo = "Museo Nicola Bottacin", indirizzo = "Palazzo Zuckermann - Corso Garibaldi, 33, Padova", telefono = "0498205675", costobiglietto = "14")
session.add_all([mus1, mus2])
session.commit()

aper1 = Aperture(id_apertura = 1, codicemuseo = 33, giorno = "Martedì", oraapertura = "09:00", orachiusura = "19:00")
aper2 = Aperture(id_apertura = 2, codicemuseo = 33, giorno = "Mercoledì", oraapertura = "09:00", orachiusura = "19:00")
aper3 = Aperture(id_apertura = 3, codicemuseo = 33, giorno = "Giovedì", oraapertura = "09:00", orachiusura = "19:00")
aper4 = Aperture(id_apertura = 4, codicemuseo = 33, giorno = "Venerdì", oraapertura = "09:00", orachiusura = "19:00")
aper5 = Aperture(id_apertura = 5, codicemuseo = 33, giorno = "Sabato", oraapertura = "09:00", orachiusura = "19:00")
aper6 = Aperture(id_apertura = 6, codicemuseo = 33, giorno = "Domenica", oraapertura = "09:00", orachiusura = "19:00")
aper7 = Aperture(id_apertura = 7, codicemuseo = 34, giorno = "Martedì", oraapertura = "09:00", orachiusura = "19:00")
aper8 = Aperture(id_apertura = 8, codicemuseo = 34, giorno = "Mercoledì", oraapertura = "09:00", orachiusura = "19:00")
aper9 = Aperture(id_apertura = 9, codicemuseo = 34, giorno = "Giovedì", oraapertura = "09:00", orachiusura = "19:00")
aper10 = Aperture(id_apertura = 10, codicemuseo = 34, giorno = "Venerdì", oraapertura = "09:00", orachiusura = "19:00")
aper11 = Aperture(id_apertura = 11, codicemuseo = 34, giorno = "Sabato", oraapertura = "09:00", orachiusura = "19:00")
aper12 = Aperture(id_apertura = 12, codicemuseo = 34, giorno = "Domenica", oraapertura = "09:00", orachiusura = "19:00")
session.add_all([aper1, aper2, aper3, aper4, aper5, aper6, aper7, aper8, aper9, aper10, aper11, aper12])
session.commit()

dip1 = Dipendenti(id_dipendente = 1, mansione = "Sicurezza", dataassunzione = "2011-11-01", cittàresidenza = "Padova", indirizzoresidenza = "Viale Francesco Crispi, 14", cf = "DCMNTN83H27A124B", codicetitolo = 3, codicemuseo = 33, id_persona = 4)
session.add(dip1)
dip2 = Dipendenti(id_dipendente = 2, mansione = "Sicurezza", dataassunzione = "2018-01-01", cittàresidenza = "Padova", indirizzoresidenza = "Via Vercelli, 22", cf = "STWFRN85H15Z468D", codicetitolo = 5, codicemuseo = 34, id_persona = 10)
session.add(dip2)

coll1 = Collaboratori(id_collaboratore = 1, mansione = "Guida", cittàresidenza = "Vercelli", indirizzoresidenza = "Via Toscana, 11", cf = "NSCNDA89C58Z321B", codicetitolo = 4, codicemuseo = 33, id_persona = 5)
coll2 = Collaboratori(id_collaboratore = 2, mansione = "Guida", cittàresidenza = "Roma", indirizzoresidenza = "Via Tuscolana, 1221", cf = "NRDFRN87T44H501J", codicetitolo = 5, codicemuseo = 34, id_persona = 9)
session.add_all([coll1, coll2])
session.commit()

dir1 = Direttori(id_direttore = 1, dataassunzione = "1996-04-01", datanomina = "2018-09-01", cittàresidenza = "Padova", indirizzoresidenza = "Via Cavour, 102", cf = "RSSMRA70S11A230U", codicetitolo = 5, codicemuseo = 33, id_persona = 3)   
dir2 = Direttori(id_direttore = 2, dataassunzione = "2014-02-01", datanomina = "2014-02-01", cittàresidenza = "Padova", indirizzoresidenza = "Via Candeo, 12", cf = "NGTJHN63E18Z512K", codicetitolo = 5, codicemuseo = 34, id_persona = 7)
session.add_all([dir1, dir2])
session.commit()

cor1 = CorrentiArtistiche(id_corrente = 1, nomeca = "Rinascimento", datainizio = "Inizio 1400", datafine = "Fine 1500", influenze = "Influenzato principalmente da Andrea Mantegna, Piero della Francesca e Antonello da Messina", codicemuseo = 33, id_artista = 3)
cor2 = CorrentiArtistiche(id_corrente = 2, nomeca = "Rinascimento Maturo", datainizio = "Inizio 1500", datafine = "Fine 1500", influenze = "Sviluppato principalmente a Firenze, Milano, Venezia e Roma da Leonardo da Vinci, Michelangelo Buonarroti, Raffaello Sanzio e Tiziano Vecellio", codicemuseo = 34, id_artista = 4)
session.add_all([cor1, cor2])
session.commit()

to1 = TipologieOpere(codice = 1, nometipologia = "Olio su tela", tecnica = "Tecnica che prevede l'utilizzo di pigmenti in polvere sciolti in olio vegetale")
to2 = TipologieOpere(codice = 2, nometipologia = "Affresco", tecnica = "Tecnica di pittura murale con pigmenti diluiti in acqua e applicati su intonaco fresco")
session.add_all([to1, to2])
session.commit()

op1 = Opere(id_opera = 1, datarealizzazione = 1511, titoloopera = "Miracolo del marito geloso", disponibilità = True, codicemuseo = 34, id_artista = 3, codice = 2)
op2 = Opere(id_opera = 2, datarealizzazione = 1460, titoloopera = "Madonna col Bambino dormiente", disponibilità = True, codicemuseo = 33, id_artista = 4, codice = 1)
session.add_all([op1, op2])
session.commit()

da1 = DescrizioniArtistiche(id_desart = 1, descrizionea = "Rappresenta una donna accusata ingiustamente di adulterio, accoltellata dal marito geloso, e successivamente resuscitata da Sant'Antonio. Nonostante la complessità tecnica, quest'opera è capace di rappresentare al meglio la drammaticità della scena.", id_opera = 1)
da2 = DescrizioniArtistiche(id_desart = 2, descrizionea = "Viene raffigurata la Vergine seduta a terra con il Bambino in braccio, il tutto in un panorama agreste molto ampio e luminoso. Un'opera con un tono intimo e familiare che tuttavia allude alla morte e passione di Gesù.", id_opera = 2)
session.add_all([da1, da2])
session.commit()

ds1 = DescrizioniStoriche(id_desstor = 1, descriziones = "Tiziano si rifugia a Padova nel 1511 in fuga dalla peste presente a Venezia. Qui gli viene affidato l'incarico di compiere tre grandi affreschi nella sala principale della Scuola del Santo, opera completata a fine 1511.", id_opera = 1)
ds2 = DescrizioniStoriche(id_desstor = 2, descriziones = "Quest'opera a lungo attribuita a Marco Basaiti, venne riconosciuta nel 1928 e attribuita al Bellini. Il capolavoro della tarda maturità dell'artista è dove si realizza per la prima volta una fusione tra figure sacre e sfondo.", id_opera = 2)
session.add_all([ds1, ds2])
session.commit()

conn = engine.connect()

#1 - Update del cognome di una riga della tabella Persone da "Verdi" a "Bianchi" utilizzando session
session.query(Persone).filter(Persone.cognome == "Verdi").update({"cognome" : "Bianchi"}, synchronize_session="fetch")

#2 - Update del cognome di una riga della tabella Persone da "Bianchi" a "Rossi" utilizzando update in ORM 2.0
upd=update(Persone).where(Persone.cognome == "Bianchi").values(cognome="Rossi").execution_options(synchronize_session="fetch")
session.execute(upd)

#3 - Cancellazione di una riga nella tabella "Persone" con cognome="Buonarroti" utilizzando session
persone = session.query(Persone).filter(Persone.cognome == "Buonarroti").first()
session.delete(persone)

#4 - Cancellazione di una riga nella tabella "Persone" con cognome="Da Vinci" utilizzando delete in ORM 2.0
dlt = delete(Persone).where(Persone.cognome == "Da Vinci").execution_options(synchronize_session="fetch")
session.execute(dlt)
session.commit()

#5 - Select su persone e dipendenti che restituisca il cognome e la mansione di tutti i dipendenti.
selezione = select(Persone.cognome, Dipendenti.mansione).join(Dipendenti).order_by(Persone.id_persona, Dipendenti.id_persona)
risultato = session.execute(selezione)
riga=risultato.fetchall()
print(riga)

#6 - Select di tutte le opere di un artista e il museo in cui sono esposte
subq = select(Artisti.id_artista).join(Persone).where(Persone.cognome == "Vecellio").subquery()
sel = select(Opere.titoloopera, Musei.nomemuseo).join(subq, Opere.id_artista == subq.c.id_artista).join(Musei).order_by(Musei.codicemuseo, Opere.codicemuseo)
ris = session.execute(sel)
riga = ris.fetchall()
print(riga)
