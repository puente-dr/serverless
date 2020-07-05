const twilio = require('twilio');

const twilioClient = twilio(
  process.env.TWILIO_SID,
  process.env.TWILIO_TOKEN
);

let user = '+14075551212';

function sendMessage(user, message) {
  return twilioClient.messages.create({
    from: process.env.TWILIO_NUMBER,
    to: user,
    body: message,
  });
}

exports.handler = async function(event, context, callback) {
  try {
    const message = await sendMessage(user, 'This is a message');
    console.log('message', message);
    callback(null, {result: 'success'});
  } catch (error) {
    console.log('error', error);
    callback("error");
  }
};