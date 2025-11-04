from datetime import date
from pydantic import BaseModel

class RegisterRequest(BaseModel):
    userName: str
    passWord: str

class LoginRequest(BaseModel):
    userName: str
    passWord: str

class KeywordRequest(BaseModel):
    keyWord: str

class UserInfoRequest(BaseModel):  
    fullName: str
    gender: str
    age: int
    height: int
    weight: int
    weightTarget: int
    dateTarget: date
    Accountid: int
    ActivityLevelid: int
    Dietid: int
    LimitFoodid: list[int]
    HealthStatusid: list[int]

class IngredientItem(BaseModel):
    ingredientId: int
    weight: float

class DishRequest(BaseModel):
    name: str
    thumbnail: str
    isConfirm: int = 0
    description: str
    preparationSteps: str
    cookingSteps: str
    ingredients: list[IngredientItem]
    hashtagId: list[int]

class AddMealRequest(BaseModel):
    userId: int
    dishId: int
    mealType: str
    weight: float
    date: date

class AddDrinkRequest(BaseModel):
    userId: int
    unitDrinkId: int
    amount: float
    date: date

class AddExerciseRequest(BaseModel):
    userId: int
    exerciseId: int
    time: int
    levelExercise: str
    date: date

class IngredientRequest(BaseModel):
    name: str
    thumbnail: str
    baseUnit: str
    gramPerUnit: float
    isConfirm: int
    kcal: int
    carbs: float
    sugar: float
    fiber: float
    protein: float
    saturatedFat: float
    monounSaturatedFat: float
    polyunSaturatedFat: float
    transFat: float
    cholesterol: float
    vitaminA: float
    vitaminD: float
    vitaminC: float
    vitaminB6: float
    vitaminB12: float
    vitaminE: float
    vitaminK: float
    choline: float
    canxi: float
    fe: float
    magie: float
    photpho: float
    kali: float
    natri: float
    zn: float
    water: float
    caffeine: float
    alcohol: float

class UpdateIngredientRequest(BaseModel):
    id:int
    name: str
    thumbnail: str
    baseUnit: str
    gramPerUnit: float
    isConfirm: int
    kcal: int
    carbs: float
    sugar: float
    fiber: float
    protein: float
    saturatedFat: float
    monounSaturatedFat: float
    polyunSaturatedFat: float
    transFat: float
    cholesterol: float
    vitaminA: float
    vitaminD: float
    vitaminC: float
    vitaminB6: float
    vitaminB12: float
    vitaminE: float
    vitaminK: float
    choline: float
    canxi: float
    fe: float
    magie: float
    photpho: float
    kali: float
    natri: float
    zn: float
    water: float
    caffeine: float
    alcohol: float

class AddNotificationRequest(BaseModel):
    senderId: int
    receiverId: int
    type: str
    content: str
    relatedId: int