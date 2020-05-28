//carico il file db.js => tira su la connessione
const connect_to_db = require('./db');

// GET BY TALK HANDLER
//tiro su il modello dati
const talk = require('./Talk');

module.exports.get_by_tag = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    //stampo il file ricevuto
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    //estraggo il payload che è incapsulato
    if (event.body) {
        //traduco il body da stringa a json
        body = JSON.parse(event.body)
    }
    // set default
    //gestione dell'errore, senon c'è un tag
    if(!body.idx) {
        callback(null, {
            //http 500 fallimento
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks. Tag is null.'
        })
    }
    //paginazione, evito di tornare mille cose, se l'utente non lo chiede uso un default
    if (!body.doc_per_page) {
        body.doc_per_page = 10
    }
    //se non definisco page, mando la prima pagina
    if (!body.page) {
        body.page = 1
    }
    
    connect_to_db().then(() => {
        console.log('=> get_all talks');
        var i = 0;
        var tag ;
        var temp;
        var dict = {};
        var j;
        console.log('lunghezza body:'+body.idx.length);
            /*tag = talk.find({_id: body.idx[i]})
                        .then(talks => {
                   tag = talks[0].tags
                   console.log('tag: '+temp);
                   for (j = 0; j <tag.length; j++) {
                        if(!dict[tag[j]]){
                        dict[tag[j]] = 1
                        }
                        else
                        {
                        dict[tag[j]] = dict[tag[j]] +1
                        }
                    }
                    console.log('dictionary: '+JSON.stringify(dict));
                    var obj    = dict
                    var keys   = Object.keys(obj);
                    var lowest = Math.max.apply(null, keys.map(function(x) { return obj[x]} ));
                    var match  = keys.filter(function(y) { return obj[y] === lowest });
                    if(match.length>1){
                        match = match[0]
                    }
                    console.log('match: '+match);
                })
        
        */

       
        var match
        var times   = body.idx.length;
        var current = 0;
        
        (function nextLap() {
            
        
        
        console.log('current: '+current);
        console.log('body.idx[current]: '+body.idx[current]);
        if (current >= times) {
            console.log('dictionary: '+JSON.stringify(dict));
            var obj    = dict
            var keys   = Object.keys(obj);
            var lowest = Math.max.apply(null, keys.map(function(x) { return obj[x]} ));
            match  = keys.filter(function(y) { return obj[y] === lowest });
            if(match.length>1){
                match = match[0]
            }
            console.log('match: '+match);
            talk.find({tags: match})
            .skip((body.doc_per_page * body.page) - body.doc_per_page)
            .limit(body.doc_per_page)
            .then(talks => {
                    callback(null, {
                        statusCode: 200,
                        body: JSON.stringify(talks)
                    })
                }
            )
            .catch(err =>
                callback(null, {//errore
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks.'
                })
            );
        }
        else{
        tag = talk.find({_id: body.idx[current]})
            .then(talks => {
                console.log('talks: '+talks);
                tag = talks[0].tags
                console.log('tag: '+tag);
                for (j = 0; j <tag.length; j++) {
                    if(!dict[tag[j]]){
                        dict[tag[j]] = 1
                    }
                    else
                    {
                        dict[tag[j]] = dict[tag[j]] +1
                    }
                }
            // do stuff
            ++current;
            nextLap();
            })
            .catch(err =>callback(null, {//errore
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks.'
                }));
            }})();
        
        //var match = "5be32167a2dcc08470287a6029b7e4c5"
        
    });
};