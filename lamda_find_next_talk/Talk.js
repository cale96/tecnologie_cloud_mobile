//definisce il modello base
//carico libreria con sintassi nodeJs
const mongoose = require('mongoose');

//definisco schema su mongoDB
const talk_schema = new mongoose.Schema({
    _id: String,
    title: String,
    url: String,
    details: String,
    main_author: String,
    tags: [String],
    watch_next_ids: [String]
}, { collection: 'teddy_data' });

module.exports = mongoose.model('talk', talk_schema);