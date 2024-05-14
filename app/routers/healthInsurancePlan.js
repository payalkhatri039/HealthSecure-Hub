const express = require("express");
const healthInsurancePlanController = require("./../controllers/healthInsurancePlan.js");
const router = express.Router();
const auth = require("./../controllers/tokenVerification.js");

router.use(auth.verifyToken);
router
  .route("/")
  .get(healthInsurancePlanController.get)
  .post(healthInsurancePlanController.post);

router
  .route("/:id")
  .get(healthInsurancePlanController.getById)
  .delete(healthInsurancePlanController.deleteById)
  .patch(healthInsurancePlanController.patchById);

module.exports = router;
