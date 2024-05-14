const app = require("./app/app.js");
const port = 3001;

app.listen(port, () => {
  console.log(`Server is listening on port ${port}`);
});
