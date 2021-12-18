var faker = require("faker");
var uuid = require("uuid");
var AWS = require("aws-sdk");
AWS.config.update({ region: "eu-central-1" });

var kinesis = new AWS.Kinesis();

const STREAM_NAME = "GroceryOrderStream";
console.log("Populating Kinesis %s strean with orders.", STREAM_NAME);

var createItems = () => Array.from(Array(Math.ceil(Math.random() * 5)).keys()).map(() => faker.commerce.product());

var createOrder = () => {
  return {
    order_id: uuid.v4(),
    user_id: "uid_" + faker.datatype.number(500),
    total_cost: faker.commerce.price(10, 200),
    items: createItems(),
  };
};

var main = async () => {
  var totalRecords = 0;
  while (true) {
    var records = [];
    for (var i = 0; i < 100; i++) {
      var order = createOrder();

      var payload = JSON.stringify(order);

      var params = {
        Data: payload,
        PartitionKey: order.order_id,
      };

      records.push(params);
    }

    var response = await kinesis
      .putRecords({
        Records: records,
        StreamName: STREAM_NAME,
      })
      .promise();

    totalRecords += records.length;
    console.log(`Send ${records.length} order, total: ${totalRecords}`);
  }
};

main();
