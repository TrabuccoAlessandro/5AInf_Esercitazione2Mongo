//Installato e richiesto il modulo di mongodb
let mongo = require("mongodb");
//Prelevo la parte del modulo per la gestione del client mongo
let mongoClient = mongo.MongoClient;
let  urlServerMongoDb = "mongodb://localhost:27017/";


let http = require("http");
let url = require("url");

let database = "5AInf_3";

//DEFINISCO IL SERVER
let json, op;
let server = http.createServer(function(req, res){
    //Avverto il browser che ritorno un oggetto JSON
    res.setHeader('Content-Type', 'application/json');

    //Decodifico la richiesta ed eseguo la query interessata
    let scelta = (url.parse(req.url)).pathname;
    switch(scelta){
        case "/i1":
            insertMany(res,"utenti",[
                {_id:1, nome:"Carlo", cognome:"Ferrero", residenza:"Fossano", anni:54},
                {_id:2, nome:"Leopoldo", cognome:"Marengo", residenza:"Cuneo", anni:65},
                {_id:3, nome:"Mattia", cognome:"Manzo", residenza:"Bra", anni:22},
                {_id:4, nome:"Rosanna", cognome:"Gelso", residenza:"Savigliano", anni:35},
                {_id:5, nome:"Margherita", cognome:"Pea", residenza:"Cuneo", anni:18},
                {_id:6, nome:"Leone", cognome:"Manzo", residenza:"Fossano", anni:43},
                {_id:7, nome:"Albana", cognome:"Renzi", residenza:"Bra", anni:48},
                {_id:8, nome:"Elisa", cognome:"Basso", residenza:"Savigliano", anni:31}
            ],{});
            break;
        case "/i2":
            insertMany(res,"transazioni",[
                {mittente:4, destinatario:3, somma:54.6, data:new Date("2020-08-16")},
                {mittente:3, destinatario:5, somma:20.0, data:new Date("2020-09-18")},
                {mittente:4, destinatario:3, somma:5.60, data:new Date("2020-10-23")},
                {mittente:5, destinatario:2, somma:14.3, data:new Date("2020-12-03")},
                {mittente:2, destinatario:6, somma:12.0, data:new Date("2021-01-14")},
                {mittente:8, destinatario:5, somma:100.0, data:new Date("2021-01-20")},
                {mittente:1, destinatario:3, somma:45.0, data:new Date("2021-01-22")},
                {mittente:8, destinatario:2, somma:34.8, data:new Date("2021-01-22")},
                {mittente:3, destinatario:7, somma:200.0, data:new Date("2021-01-27")}
            ],{});
                break;
        case "/q1":
            find(res,"utenti",{residenza:"Fossano"},{});
            break;
        case "/q2":
            find(res,"utenti",{$or:[{nome:/^L/},{nome:/^C/}],anni:{$gt:50}},{});
            break;
        case "/q3":
            limit(res,"utenti",{cognome:/o$/},{nome:1, cognome:1}, 2);
            break;
        case "/q4":
            op=[
                {$group:{_id:{residenza:"$residenza"},media:{$avg:"$anni"}}}
            ];
            aggregate(res,"utenti",op);
            break;
        case "/q5":
            let cogn = "Gelso";
            find2(res, "utenti", {cognome:cogn}, {_id:1}, function(ris) {
                //res.end(JSON.stringify(ris));
                //Prendere il codice
                console.log(ris); //ARRAY
                let id = ris[0]._id;
                console.log(id);
                console.log({codP: id});
                find(res, "transazioni", {mittente: parseInt(id)}, {data:0});
            });
            break;
        case "/q6":
            cont(res,"transazioni",{somma:{$gt:20}})
            break;
        case "/q7":
            find2(res,"utenti",{nome:"Mattia",cognome:"Manzo"},{},function (ris){
               let id = ris[0]._id;
               let opzioni = [
                   {$match:{destinatario:id}},
                   {$group:{_id:"$destinatario", entrate:{$sum: "$somma"}}}
               ];
               aggregate2(res,"transazioni",opzioni,function (ris){
                   let entrate = ris[0].entrate;
                   opzioni = [
                       {$match:{mittente:id}},
                       {$group:{_id:"mittente", uscite:{$sum: "$somma"}}}
                   ]
                   aggregate2(res,"transazioni",opzioni,function (ris){
                       let uscite = ris[0].uscite;
                       let bilancio = entrate-uscite;
                       json = {cod:1,desc:"Il resoconto di Mattia Manzo è "+ bilancio};
                       res.end(JSON.stringify(json));
                   });
               });
            });
            break;
        case "/q8":
            //Raggruppa i destinatari e somma per ciascuno il denaro ricevuto
            op=[
                {$group:{_id:{destinatario:"$destinatario"},sommaDen:{$sum:"$somma"}}}
            ]
            aggregate(res,"transazioni",op);
            break;
        case "/q9":
            let data = new Date("2021-01-01");
            find(res,"transazioni",{data:{$gt:data}},{});
            break;
        
        default:
            json = {cod:-1, desc:"Nessuna query trovata con quel nome"};
            res.end(JSON.stringify(json));
    }
});

server.listen(8888, "127.0.0.1");
console.log("Il server è in ascolto sulla porta 8888");

function creaConnessione(nomeDb, response, callback){
    console.log(mongoClient);
    let promise = mongoClient.connect(urlServerMongoDb);
    promise.then(function(connessione){
        callback(connessione, connessione.db(nomeDb))
    });
    promise.catch(function(err){
        json = {cod:-1, desc:"Errore nella connessione"};
        response.end(JSON.stringify(json));
    });
}

function find2(res, col, obj, select, callback){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).find(obj).project(select).toArray();
        promise.then(function(ris){
            conn.close();
            callback(ris);
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function find(res, col, obj, select){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).find(obj).project(select).toArray();
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

/*
    aggregate -> aggregazione di funzioni di ricerca

    opzioni -> array di oggetti dove ogni oggetto è un 
            filtro che vogliamo applicare alla collezione

*/
function aggregate(res, col, opzioni){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).aggregate(opzioni).toArray();
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function aggregate2(res, col, opzioni,callback){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).aggregate(opzioni).toArray();
        promise.then(function(ris){
            conn.close();
            callback(ris);
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function limit(res, col, obj, select, n){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).find(obj).project(select).limit(n).toArray();
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function sort(res, col, obj, select, orderby){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).find(obj).project(select).sort(orderby).toArray();
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function cont(res, col, query){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).countDocuments(query);
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function cont2(res, col, query){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).count(query);
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function insertOne(res, col, obj){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).insertOne(obj); 
        promise.then(function(ris){
            json = { cod:1, desc:"Insert in esecuzione", ris };
            res.end(JSON.stringify(json));
            conn.close();
        });
        promise.catch(function(err){
            obj = { cod:-2, desc:"Errore nell'inserimento"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function insertMany(res, col, array){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).insertMany(array); 
        promise.then(function(ris){
            json = { cod:1, desc:"Insert in esecuzione", ris };
            res.end(JSON.stringify(json));
            conn.close();
        });
        promise.catch(function(err){
            obj = { cod:-2, desc:"Errore nell'inserimento"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function remove(res, col, where){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).deleteMany(where); 
        promise.then(function(ris){
            json = { cod:1, desc:"Remove in esecuzione", ris };
            res.end(JSON.stringify(json));
            conn.close();
        });
        promise.catch(function(err){
            obj = { cod:-2, desc:"Errore nella cancellazione"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}