import firebase_admin
from firebase_admin import credentials, messaging

cred = credentials.Certificate("C:/Users/Admin/Desktop/MyHealth/foodai-2a6fb-firebase-adminsdk-fbsvc-e4960dc1b2.json")
firebase_admin.initialize_app(cred)

device_token = "eUTg2c9UR5uoWyy6GgZt-P:APA91bHr1pCZmvtSm1Nahw6tHPm-2LA5foMGCuu-heYUrx8Dl4PpEE4GtmV60pN0GtHtbLCLcOr13Vpa5dVU9sPOEdI0zIUIU45k8nSniFTFoIFQNAc5QPc"

message = messaging.Message(
    notification=messaging.Notification(
        title="Thông báo từ server",
        body="Đây là thông báo được gửi bằng Firebase Admin SDK!",
    ),
    token=device_token,
)

response = messaging.send(message)
print("✅ Gửi thành công:", response)
