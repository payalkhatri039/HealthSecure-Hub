var redis = require("redis");
var validate = require("jsonschema").validate;
var schema = require("./../json_schema/schema.json");
const md5 = require("md5");
const tokenVerification = require("./tokenVerification");
const rabbit = require("../services/rabbitmq.service.js");
var config = require("../json_schema/config.json");

const generateETag = (healthInsurancePlanObj) => {
  const md5Hash = md5(JSON.stringify(healthInsurancePlanObj));
  return `"${md5Hash}"`;
};

(async () => {
  redisClient = redis.createClient();
  redisClient.on("error", (error) => console.error(`Error : ${error}`));
  await redisClient.connect();
})();




const get = async (request, response) => {
  const authToken = request.headers.authorization;
  if (!authToken) {
    return response
      .status(401)
      .send("Unauthorized: Missing authorization token");
  }

  const result = [];
  const keys = await redisClient.keys("plan:*");

  for (const key of keys) {
    const data = await redisClient.get(key);
    const parsedData = JSON.parse(data);
    // Fetch values associated with planCostShares
    const planCostSharesValue = await redisClient.get(
      parsedData.planCostShares
    );
    if (planCostSharesValue) {
      parsedData.planCostShares = JSON.parse(planCostSharesValue);
    }

    // Fetch values associated with linkedPlanServices
    const linkedPlanServicesValues = await Promise.all(
      parsedData.linkedPlanServices.map(async (linkedPlanServiceKey) => {
        const linkedPlanServiceData = await redisClient.get(
          linkedPlanServiceKey
        );
        const linkedServiceValue = await redisClient.get(
          JSON.parse(linkedPlanServiceData).linkedService
        );
        const planserviceCostSharesValue = await redisClient.get(
          JSON.parse(linkedPlanServiceData).planserviceCostShares
        );

        if (linkedServiceValue && planserviceCostSharesValue) {
          const linkedServiceData = JSON.parse(linkedServiceValue);
          const planserviceCostSharesData = JSON.parse(
            planserviceCostSharesValue
          );

          return {
            linkedService: linkedServiceData,
            planserviceCostShares: planserviceCostSharesData,
            _org: linkedPlanServiceData._org,
            objectId: linkedPlanServiceData.objectId,
            objectType: linkedPlanServiceData.objectType,
          };
        }
      })
    );

    // Replace keys with values for linkedPlanServices
    parsedData.linkedPlanServices = linkedPlanServicesValues.filter(Boolean); // Filter out undefined values

    result.push(parsedData);
  }

  response.send(result);
};

const getById = async (request, response) => {
  const authToken = request.headers.authorization;

  if (!authToken) {
    return response
      .status(401)
      .send("Unauthorized: Missing authorization token");
  }

  var key = "plan:" + request.params.id;
  const result = await redisClient.get(key);

  const ifNoneMatchEtag = request.headers["if-none-match"];

  var planEtag;
  if (result) {
    planEtag = generateETag(JSON.parse(result));
  }

  if (result && ifNoneMatchEtag && ifNoneMatchEtag === planEtag) {
    return response.status(304).send();
  }

  if (!result) {
    return response.status(404).send();
  }

  const parsedResult = JSON.parse(result);

  // Fetch values associated with planCostShares
  const planCostSharesValue = await redisClient.get(
    parsedResult.planCostShares
  );
  if (planCostSharesValue) {
    parsedResult.planCostShares = JSON.parse(planCostSharesValue);
  }

  // Fetch values associated with linkedPlanServices
  const linkedPlanServicesValues = await Promise.all(
    parsedResult.linkedPlanServices.map(async (linkedPlanServiceKey) => {
      const linkedPlanServicesValue = await redisClient.get(
        linkedPlanServiceKey
      );
      const linkedPlanServiceData = JSON.parse(linkedPlanServicesValue);
      const linkedServiceValue = await redisClient.get(
        linkedPlanServiceData.linkedService
      );
      const planserviceCostSharesValue = await redisClient.get(
        linkedPlanServiceData.planserviceCostShares
      );

      if (linkedServiceValue && planserviceCostSharesValue) {
        linkedPlanServiceData.linkedService = JSON.parse(linkedServiceValue);
        linkedPlanServiceData.planserviceCostShares = JSON.parse(
          planserviceCostSharesValue
        );
      }

      return linkedPlanServiceData;
    })
  );

  // Replace keys with values for linkedPlanServices
  parsedResult.linkedPlanServices = linkedPlanServicesValues;

  response.setHeader("ETag", planEtag);
  response.status(200).send(parsedResult);
};

const post = async (request, response) => {
  const authToken = request.headers.authorization;
  if (!authToken) {
    return response
      .status(401)
      .send("Unauthorized: Missing authorization token");
  }

  var validator = validate(request.body, schema);

  if (validator.valid == true) {
    planCostShareKey =
      request.body.planCostShares.objectType +
      ":" +
      request.body.planCostShares.objectId;
    await redisClient.set(
      planCostShareKey,
      JSON.stringify(request.body.planCostShares)
    );

    const linkedPlanServicesKeys = [];
    for (let i = 0; i < request.body.linkedPlanServices.length; i++) {
      const linkedPlanService = request.body.linkedPlanServices[i];

      const linkedServiceKey =
        linkedPlanService.linkedService.objectType +
        ":" +
        linkedPlanService.linkedService.objectId;
      await redisClient.set(
        linkedServiceKey,
        JSON.stringify(linkedPlanService.linkedService)
      );

      const planserviceCostSharesKey =
        linkedPlanService.planserviceCostShares.objectType +
        ":" +
        linkedPlanService.planserviceCostShares.objectId;
      await redisClient.set(
        planserviceCostSharesKey,
        JSON.stringify(linkedPlanService.planserviceCostShares)
      );

      const linkedPlanServiceKey =
        linkedPlanService.objectType + ":" + linkedPlanService.objectId;
      const linkedPlanServiceBody = {
        linkedService: linkedServiceKey,
        planserviceCostShares: planserviceCostSharesKey,
        _org: linkedPlanService._org,
        objectId: linkedPlanService.objectId,
        objectType: linkedPlanService.objectType,
      };

      linkedPlanServicesKeys.push(linkedPlanServiceKey);

      await redisClient.set(
        linkedPlanServiceKey,
        JSON.stringify(linkedPlanServiceBody)
      );
    }

    const healthInsurancePlanKey =
      request.body.objectType + ":" + request.body.objectId;
    const healthInsurancePlanBody = {
      planCostShares: planCostShareKey,
      linkedPlanServices: linkedPlanServicesKeys,
      _org: request.body._org,
      objectId: request.body.objectId,
      objectType: request.body.objectType,
      planType: request.body.planType,
      creationDate: request.body.creationDate,
    };

    await redisClient.set(
      healthInsurancePlanKey,
      JSON.stringify(healthInsurancePlanBody)
    );
    console.log(request.body.objectId)
   const bodyToPublish = await getObjectForPubSub(request.body.objectId);
    console.log("sending message to queue to create plan....");
    // Send Message to Queue for Indexing
    const message = {
      operation: "STORE",
      body: bodyToPublish,
    };
    rabbit.producer(message);

    // Generate ETag
    const etag = generateETag(healthInsurancePlanBody);
    response.setHeader("ETag", etag);
    response.status(201).send({ objectId: request.body.objectId });
  } else {
    response
      .status(400)
      .send("Bad request. Data does not match the specified schema");
  }
};

const deletePlan = async (planKey) => {
  // Delete internal keys first
  const planData = await redisClient.get(planKey);
  if (planData) {
    const parsedPlanData = JSON.parse(planData);

    // Delete planCostShares
    if (parsedPlanData.planCostShares) {
      await redisClient.del(parsedPlanData.planCostShares);
    }
    // Delete linkedPlanServices
    for (const linkedPlanServiceKey of parsedPlanData.linkedPlanServices) {
      const linkedPlanServicesValue = await redisClient.get(
        linkedPlanServiceKey
      );
      const linkedPlanServiceData = JSON.parse(linkedPlanServicesValue);
      if (linkedPlanServiceData) {
        if (linkedPlanServiceData.linkedService) {
          await redisClient.del(linkedPlanServiceData.linkedService);
        }
        if (linkedPlanServiceData.planserviceCostShares) {
          await redisClient.del(linkedPlanServiceData.planserviceCostShares);
        }

        await redisClient.del(linkedPlanServiceKey);
      }
    }

    // Delete main plan key
    await redisClient.del(planKey);

    return true; // Indicate successful deletion
  }

  return false; // Indicate failure (key not found or already deleted)
};

const deleteById = async (request, response) => {
  const authToken = request.headers.authorization;
  if (!authToken) {
    return response
      .status(401)
      .send("Unauthorized: Missing authorization token");
  }

  const planKey = "plan:" + request.params.id;

  const bodyToPublish = await getObjectForPubSub(request.params.id);
  console.log("sending message to queue for delete....");
    // Send Message to Queue for Indexing
    const message = {
      operation: "DELETE",
      body: bodyToPublish
  }
  rabbit.producer(message);

  console.log("Deleting plan...");

  const success = await deletePlan(planKey);

  if (success) {
    response.status(204).send();
  } else {
    response.status(404).send("Plan not found or already deleted");
  }
};

const patchById = async (request, response) => {
  const authToken = request.headers.authorization;
  if (!authToken) {
    return response
      .status(401)
      .send("Unauthorized: Missing authorization token");
  }

  const planId = request.params.id;

  // Fetch existing plan data
  const planKey = "plan:" + planId;
  const existingPlanData = await redisClient.get(planKey);

  if (!existingPlanData) {
    response.status(404).send("Plan not found");
    return;
  }
  const existingPlan = JSON.parse(existingPlanData);

  const existingPlanEtag = generateETag(existingPlan);
  // Check if If-Match header matches the etag of existing data to check if the data is upto date
  const ifMatch = request.headers["if-match"];
  if (!ifMatch || ifMatch !== existingPlanEtag) {
    return response
      .status(412)
      .send(
        "Precondition Failed: If-Match header does not match current ETag. Object may have been updated"
      );
  }

  const validator = validate(request.body, schema);
  if (!validator.valid) {
    return response.status(400).send("Bad request. Invalid patch data");
  } else {
    const linkedPlanKeysSet = new Set();
    for (const linkedPlanServiceKey of existingPlan.linkedPlanServices) {
      linkedPlanKeysSet.add(linkedPlanServiceKey);
      const linkedPlanServiceValue = await redisClient.get(
        linkedPlanServiceKey
      );
      const linkedPlanServiceData = JSON.parse(linkedPlanServiceValue);
      linkedPlanKeysSet.add(linkedPlanServiceData.linkedService);
      linkedPlanKeysSet.add(linkedPlanServiceData.planserviceCostShares);
    }

    for (let i = 0; i < request.body.linkedPlanServices.length; i++) {
      const linkedPlanService = request.body.linkedPlanServices[i];

      const patchLinkedPlanServiceKey =
        linkedPlanService.objectType + ":" + linkedPlanService.objectId;
      const planserviceCostSharesKey =
        linkedPlanService.planserviceCostShares.objectType +
        ":" +
        linkedPlanService.planserviceCostShares.objectId;

      const linkedServiceKey =
        linkedPlanService.linkedService.objectType +
        ":" +
        linkedPlanService.linkedService.objectId;

      await redisClient.set(
        planserviceCostSharesKey,
        JSON.stringify(linkedPlanService.planserviceCostShares)
      );

      await redisClient.set(
        linkedServiceKey,
        JSON.stringify(linkedPlanService.linkedService)
      );

      const linkedPlanServiceBody = {
        linkedService: linkedServiceKey,
        planserviceCostShares: planserviceCostSharesKey,
        _org: linkedPlanService._org,
        objectId: linkedPlanService.objectId,
        objectType: linkedPlanService.objectType,
      };
      existingPlan.linkedPlanServices.push(patchLinkedPlanServiceKey);

      await redisClient.set(
        patchLinkedPlanServiceKey,
        JSON.stringify(linkedPlanServiceBody)
      );

      const bodyToPublish = await getObjectForPubSub(planId);
      console.log("sending message to queue to patch....");
        // Send Message to Queue for Indexing
        const message = {
          operation: "STORE",
          body: bodyToPublish
      }
      rabbit.producer(message);
 
      await redisClient.set(planKey, JSON.stringify(existingPlan));
    }
  }

  // Generate updated ETag
  const etag = generateETag(existingPlan);
  if (etag === existingPlanEtag) {
    return response.status(304).send();
  }
  response.setHeader("ETag", etag);
  response.status(200).send("Plan updated successfully");
};

const getObjectForPubSub = async(objKey) =>{
  var key = "plan:" + objKey;
  const result = await redisClient.get(key);
  const parsedResult = JSON.parse(result);

  // Fetch values associated with planCostShares
  const planCostSharesValue = await redisClient.get(
    parsedResult.planCostShares
  );
  if (planCostSharesValue) {
    parsedResult.planCostShares = JSON.parse(planCostSharesValue);
  }

  // Fetch values associated with linkedPlanServices
  const linkedPlanServicesValues = await Promise.all(
    parsedResult.linkedPlanServices.map(async (linkedPlanServiceKey) => {
      const linkedPlanServicesValue = await redisClient.get(
        linkedPlanServiceKey
      );
      const linkedPlanServiceData = JSON.parse(linkedPlanServicesValue);
      const linkedServiceValue = await redisClient.get(
        linkedPlanServiceData.linkedService
      );
      const planserviceCostSharesValue = await redisClient.get(
        linkedPlanServiceData.planserviceCostShares
      );

      if (linkedServiceValue && planserviceCostSharesValue) {
        linkedPlanServiceData.linkedService = JSON.parse(linkedServiceValue);
        linkedPlanServiceData.planserviceCostShares = JSON.parse(
          planserviceCostSharesValue
        );
      }

      return linkedPlanServiceData;
    })
  );

  // Replace keys with values for linkedPlanServices
  parsedResult.linkedPlanServices = linkedPlanServicesValues;
 return parsedResult;
}


module.exports = { get, getById, post, deleteById, patchById };
