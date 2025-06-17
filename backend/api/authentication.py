import os
from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed
from django.contrib.auth.models import User
import google.auth.transport.requests
import google.oauth2.id_token
import os
from dotenv import load_dotenv

class GoogleOAuthAuthentication(BaseAuthentication):
    def authenticate(self, request):
        load_dotenv()
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return None

        token = auth_header.split(" ")[1]

        try:
            request_adapter = google.auth.transport.requests.Request()
            id_info = google.oauth2.id_token.verify_oauth2_token(
                token,
                request_adapter,
                audience=os.getenv("GOOGLE_CLIENT_ID")  # ✅ Must match token's audience
            )

            email = id_info.get("email")
            if not email:
                raise AuthenticationFailed("Email not found in token")

            user, _ = User.objects.get_or_create(username=email, defaults={"email": email})
            return (user, None)

        except Exception as e:
            raise AuthenticationFailed(f"Google authentication failed: {str(e)}")
