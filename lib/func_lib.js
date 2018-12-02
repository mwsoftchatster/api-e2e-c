/* jshint esnext: true */
var config = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-e2e-c/config/config.js');
var email = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-e2e-c/lib/email_lib.js');


/**
 *  Setup the pool of connections to the db so that every connection can be reused upon it's release
 *
 */
var mysql = require('mysql');
var Sequelize = require('sequelize');
const sequelize = new Sequelize(config.db.name, config.db.user_name, config.db.password, {
    host: config.db.host,
    dialect: config.db.dialect,
    port: config.db.port,
    operatorsAliases: config.db.operatorsAliases,
    pool: {
      max: config.db.pool.max,
      min: config.db.pool.min,
      acquire: config.db.pool.acquire,
      idle: config.db.pool.idle
    }
});

/**
 *  Publishes message on api-user-c topic to notify of successful reception of one time keys
 */
function publishOnUserC(amqpConn, message, topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiUserC.*';
            var toipcName = `apiUserC.${topic}`;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.publish(exchange, toipcName, new Buffer(message));
        });
    }else {
        // log and send error
        email.sendApiE2ECErrorEmail('API E2E C publishOnUserC AMPQ connection was null');
    }
}

/**
 *  Publishes message on api-e2e-q topic with newly generated one time keys batch
 */
function publishOnE2EQ(amqpConn, message, topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiE2EQ.*';
            var toipcName = `apiE2EQ.${topic}`;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.publish(exchange, toipcName, new Buffer(message));
        });
    }else {
        // log and send error
        email.sendApiE2ECErrorEmail('API E2E C publishOnE2EQ AMPQ connection was null');
    }
}

/**
 *  Publishes message on api-e2e-q topic with newly generated one time keys batch
 */
function publishOnChatC(amqpConn, message, topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiChatC.*';
            var toipcName = `apiChatC.${topic}`;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.publish(exchange, toipcName, new Buffer(message));
        });
    }else {
        // log and send error
        email.sendApiE2ECErrorEmail('API E2E C publishOnChatC AMPQ connection was null');
    }
}


/**
 * Model of user_one_time_pre_key_pair table
 * 
 */
const OneTimePreKey = sequelize.define('user_one_time_pre_key_pair', {
    user_id: { 
            type: Sequelize.INTEGER,
            allowNull: false
        },
    one_time_pre_key_pair_pbk: {type: Sequelize.BLOB('long'), allowNull: false},
    one_time_pre_key_pair_uuid: {type: Sequelize.STRING, allowNull: false}
}, {
  freezeTableName: true, // Model tableName will be the same as the model name
  timestamps: false,
  underscored: true
});


/**
 *  Saves public one time keys into db
 * 
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
module.exports.uploadPublicKeys = function(req, res, amqpConn){
    var oneTimePreKeyPairPbks = JSON.parse(req.query.oneTimePreKeyPairPbks);

    OneTimePreKey.bulkCreate(oneTimePreKeyPairPbks.oneTimePreKeyPairPbks, { fields: ['user_id','one_time_pre_key_pair_pbk', 'one_time_pre_key_pair_uuid'] }).then(() => {
        var message = {
            status: config.rabbitmq.statuses.ok,
            keys: req.query.oneTimePreKeyPairPbks
        };
        // publish to api-e2e-q
        publishOnE2EQ(amqpConn, JSON.stringify(message), config.rabbitmq.topics.newUserE2EKeys);
        res.json("success");
    }).error(function(err){
        email.sendApiE2ECErrorEmail(err);
        res.json("error");
    });
};

/**
 *  Saves public one time keys into db received from api-user-c during registration
 * 
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
module.exports.saveRegistrationPublicKeys = function(message, amqpConn, topic){
    OneTimePreKey.bulkCreate(message.oneTimePreKeyPairPbks, { fields: ['user_id','one_time_pre_key_pair_pbk', 'one_time_pre_key_pair_uuid'] }).then(() => {
        var response = {
            status: config.rabbitmq.statuses.ok
        };
        // publish to api-user-c acknowledging successful message reception
        publishOnUserC(amqpConn, JSON.stringify(response), config.rabbitmq.topics.userCE2EKeys);
        // publish to api-e2e-q
        publishOnE2EQ(amqpConn, JSON.stringify(message), topic);
    }).error(function(err){
        email.sendApiE2ECErrorEmail(err);
    });
};


/**
 *  Saves public one time keys into db
 * 
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
module.exports.uploadReRegisterPublicKeys = function(req, res, amqpConn){
    var oneTimePreKeyPairPbks = JSON.parse(req.query.oneTimePreKeyPairPbks);

    console.log("req.query.userId => " + req.query.userId);

    sequelize.query('CALL DeleteOldOneTimePublicKeysByUserId(?)',
    { replacements: [ req.query.userId ],
        type: sequelize.QueryTypes.RAW }).then(result => {
            OneTimePreKey.bulkCreate(oneTimePreKeyPairPbks.oneTimePreKeyPairPbks, { fields: ['user_id','one_time_pre_key_pair_pbk', 'one_time_pre_key_pair_uuid'] }).then(() => {
                // publish to api-e2e-q
                publishOnE2EQ(amqpConn, req.query.oneTimePreKeyPairPbks, config.rabbitmq.topics.reregisterUserE2EKeys);
                res.json("success");
            }).error(function(err){
                email.sendApiE2ECErrorEmail(err);
                res.json("error");
            });
    }).error(function(err){
        email.sendApiE2ECErrorEmail(err);
        res.json("error");
    });
};


/**
 *  Removes public one time keys from db after processing message
 * 
 * (contactPublicKeyUUID UUID): UUID of one time public key that was used to process one message
 * (userPublicKeyUUID UUID): UUID of one time public key that was used to process one message
 * (amqpConn Object): RabbitMQ object that was 
 */
module.exports.deleteOneTimePublicKeysByUUID = function(message, amqpConn, topic){
    console.log("deleteOneTimePublicKeysByUUID has been called");
    sequelize.query('CALL DeleteOneTimePublicKeysByUUID(?,?)',
    { replacements: [ message.contactPublicKeyUUID, message.userPublicKeyUUID ],
        type: sequelize.QueryTypes.RAW }).then(result => {
            // publish to api-e2e-q
            publishOnE2EQ(amqpConn, JSON.stringify(message), topic);

            var response = {
                status: config.rabbitmq.statuses.ok
            };
            // publish to api-chat-c
            publishOnChatC(amqpConn, JSON.stringify(response), config.rabbitmq.topics.deleteOneTimePublicKeysByUUIDC);
    }).error(function(err){
        var response = {
            status: config.rabbitmq.statuses.error
        };
        // publish to api-chat-c
        publishOnChatC(amqpConn, JSON.stringify(response), config.rabbitmq.topics.deleteOneTimePublicKeysByUUIDC);
    });
};