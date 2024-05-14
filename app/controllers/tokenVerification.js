var axios = require( "axios");

const setRespone = (message = "", response, httpCode) => {
  response.status(httpCode);
  response.send(message);
};

async function verifyToken(req, res, next) {
  const authorizationHeader = req.headers.authorization;
  if (authorizationHeader && authorizationHeader.startsWith("Bearer ")) {
    try {
      const verify = await axios({
        url:
          "https://oauth2.googleapis.com/tokeninfo?access_token=" +
          authorizationHeader.split(" ")[1],
        method: "get",
      });
      next();
    } catch (error) {
      setRespone("Authorization failed", res, 401);
      return;
    }
  } else {
    setRespone("Invalid authorization", res, 401);
    return;
  }
}

module.exports = { verifyToken };