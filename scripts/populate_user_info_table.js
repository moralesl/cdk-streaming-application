var faker = require('faker');
var AWS = require('aws-sdk');
AWS.config.update({region: 'eu-central-1'});

var ddb = new AWS.DynamoDB({apiVersion: '2012-08-10'});


const TABLE_NAME = 'CdkStreamingApplicationStack-UserInfoTable80AC8EDD-GJKCWJGOC11D'
console.log('Populating DynamoDB %s table with users.', TABLE_NAME);

var allPromises = new Array();
for(i = 0; i <= 500; i++) {
    var params = {
        Item: {
            "user_id":  { S: 'uid_' + i }, 
            "first_name":  { S: faker.name.firstName() }, 
            "last_name":  { S: faker.name.lastName() }, 
            "email":  { S: faker.internet.email() }, 
            "street_number":  { N: faker.datatype.number(128).toString() }, 
            "street_name":  { S: faker.address.streetName() }, 
            "city":  { S: faker.address.city() }, 
            "state":  { S: faker.address.state() }, 
            "country":  { S: faker.address.country() }, 
            "postcode":  { S: faker.address.zipCode() }, 
            "latitude":  { N: faker.address.latitude() }, 
            "longitude":  { N: faker.address.longitude() }
        },
        TableName: TABLE_NAME
    };

    var promise = ddb.putItem(params).promise();
    allPromises.push(promise);
}

Promise.all(allPromises).then((_) => {
    console.log("Created %s users.", allPromises.length);
});