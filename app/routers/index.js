const healthInsurance = require("./healthInsurancePlan.js");

const value = (app) => {
  app.use("/v1/healthinsurance", healthInsurance);
};

module.exports = value;
