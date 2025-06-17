from django.urls import path
from .views import (
    PastEvalsCreate,
    PastEvalsView,
    ListUserAlgorithms,
    RunEvaluationWithDB,
    AddUserAlgorithm,
    DeleteUserAlgorithm,
    ListPublicAlgorithms,
    PreviewAlgorithm,
    UploadUserAlgorithm,
)

urlpatterns = [
    path('past-evals/', PastEvalsView.as_view(), name='past-evals'),
    path('past-evals/create/', PastEvalsCreate.as_view(), name='create-eval'),
    path('scripts/user/', ListUserAlgorithms.as_view(), name='list-user-algorithms'),
    path('scripts/run/', RunEvaluationWithDB.as_view(), name='run-algorithms'),
    path('scripts/user/add/', AddUserAlgorithm.as_view(), name='add-algorithm'),
    path('scripts/user/delete/', DeleteUserAlgorithm.as_view(), name='delete-algorithm'),
    path('scripts/public/', ListPublicAlgorithms.as_view(), name='list-public-algorithms'),
    path('scripts/preview/', PreviewAlgorithm.as_view(), name='preview-algorithm'),
    path('scripts/user/upload/', UploadUserAlgorithm.as_view(), name='upload-algorithm'),
]
