from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from models import UserInfoRequest,DishRequest,RegisterRequest,LoginRequest,AddMealRequest,AddDrinkRequest,AddExerciseRequest,IngredientRequest,UpdateIngredientRequest,AddNotificationRequest
from datetime import date
from logic import insert_userinfo_to_db, register_account, login_account,login_admin,similar_food,find_food,insert_dish_to_db,find_dish,similar_dish,insert_meal_to_db,insert_drink_to_db,find_exercise,insert_exercise_to_db,total_kcal_exercise,total_nutri_meal,stat_drink_in_day,stat_exercise_in_day,stat_meal_in_day,delete_exercise_of_user,delete_drink_of_user,delete_meal_of_user,get_dish_by_id,update_dish_in_db,get_required_index_by_id,get_total_water,get_ingredient_by_id,insert_ingredient_to_db,update_ingredient_in_db,get_user_info_by_id,update_userinfo_in_db,insert_notification_to_db

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/create_userinfo")
async def create_userinfo(data: UserInfoRequest):
    result = insert_userinfo_to_db(data)
    if not result['success']:
        raise HTTPException(status_code=500, detail=result['error'])

    return {
        "message": "User info created successfully",
        "idUserInfo": result["id"]
    }

@app.patch("/update_userinfo")
async def update_userinfo_api(id: int,data: UserInfoRequest):
    try:
        result = update_userinfo_in_db(id,data)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])
        return {
            "message": "Update userinfo successful",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.post("/register_account")
async def register_account_api(data: RegisterRequest):
    if not data.userName or not data.passWord:
        raise HTTPException(status_code=400, detail="Missing username or password")

    try:
        result = register_account(data.userName, data.passWord)

        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])

        return {
            "message": "Account created successfully",
            "accountId": result["id"]
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.post("/login")
async def login_api(data: LoginRequest):
    if not data.userName or not data.passWord:
        raise HTTPException(status_code=400, detail="Missing username or password")
    try:
        result = login_account(data.userName, data.passWord)
        if not result['success']:
            raise HTTPException(status_code=401, detail=result['error'])

        response = {
            "message": "Login successful",
            "account": result["account"]
        }

        if "idUserInfo" in result:
            response["idUserInfo"] = result["idUserInfo"]

        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    
@app.get("/find_food")
async def query_food_api(keyWord: str, page: int = 1, limit: int = 20):
    try:
        result = find_food(keyWord)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])

        items = result["similar"]

        start = (page - 1) * limit
        end = start + limit
        paginated_items = items[start:end]

        return {
            "message": "Find successful",
            "similar": paginated_items
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/similar_food")
async def find_similar_food_api(keyWord: str):
    if not keyWord:
        raise HTTPException(status_code=400, detail="Missing keyword")
    try:
        result = similar_food(keyWord)
        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])

        return {
            "message": "Find successful",
            "similar": result["similar"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    
@app.get("/find_dish")
async def query_dish_api(keyWord: str, page: int = 1, limit: int = 20):
    try:
        result = find_dish(keyWord)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])

        items = result["similar"]

        start = (page - 1) * limit
        end = start + limit
        paginated_items = items[start:end]

        return {
            "message": "Find successful",
            "similar": paginated_items
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/similar_dish")
async def find_similar_dish_api(keyWord: str):
    if not keyWord:
        raise HTTPException(status_code=400, detail="Missing keyword")
    try:
        result = similar_dish(keyWord)
        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])

        return {
            "message": "Find successful",
            "similar": result["similar"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
        
@app.post("/login_admin")
async def login_admin_api(data: LoginRequest):
    if not data.userName or not data.passWord:
        raise HTTPException(status_code=400, detail="Missing username or password")

    result = login_admin(data.userName, data.passWord)

    if not result['success']:
        raise HTTPException(status_code=401, detail=result['error'])

    return {"message": "Login successful"}

@app.post("/add_dish")
async def add_dish_api(data: DishRequest):
    if not data.name or not data.ingredients: 
        raise HTTPException(status_code=400, detail="Missing name dish or ingredients")

    result = insert_dish_to_db(data)

    if not result['success']:
        raise HTTPException(status_code=401, detail=result['error'])

    return {"message": "Create dish successful"}

@app.post("/add_meal")
async def add_meal_api(data: AddMealRequest):
    if not data.mealType or not data.dishId or not data.userId or not data.weight or not data.date: 
        raise HTTPException(status_code=400, detail="Missing data")

    result = insert_meal_to_db(data)

    if not result['success']:
        raise HTTPException(status_code=401, detail=result['error'])

    return {"message": "Create meal successful"}

@app.post("/add_drink")
async def add_drink_api(data: AddDrinkRequest):
    if not data.amount or not data.unitDrinkId or not data.userId or not data.date: 
        raise HTTPException(status_code=400, detail="Missing data")

    result = insert_drink_to_db(data)

    if not result['success']:
        raise HTTPException(status_code=401, detail=result['error'])

    return {"message": "Create drink successful"}

@app.post("/add_exercise")
async def add_exercise_api(data: AddExerciseRequest):
    if not data.time or not data.exerciseId or not data.userId or not data.levelExercise or not data.date: 
        raise HTTPException(status_code=400, detail="Missing data")

    result = insert_exercise_to_db(data)

    if not result['success']:
        raise HTTPException(status_code=401, detail=result['error'])

    return {"message": "Create exercise successful"}

@app.get("/get_exercise")
async def get_exercise_api():
    try:
        result = find_exercise()
        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])

        return {
            "message": "Find successful",
            "exercises": result["exercises"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/total_kcal_exercise")
async def get_kcal_exercise_api(date: date, userId: int):
    try:
        result = total_kcal_exercise(date,userId)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])

        return {
            "message": "Find successful",
            "totalKcal": result["totalKcal"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/total_nutri_meal")
async def get_nutri_meal_api(date: date, userId: int):
    try:
        result = total_nutri_meal(date,userId)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])

        return {
            "message": "Find successful",
            "nutriMeal": result["nutriMeal"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/stat_drink")
async def get_stat_drink_api(date: date, userId: int):
    try:
        result = stat_drink_in_day(date,userId)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])

        return {
            "message": "Find successful",
            "statDrinks": result["statDrinks"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/stat_exercise")
async def get_stat_exercise_api(date: date, userId: int):
    try:
        result = stat_exercise_in_day(date,userId)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])

        return {
            "message": "Find successful",
            "statExercises": result["statExercises"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    
@app.get("/stat_meal")
async def get_stat_meal_api(date: date, userId: int, mealType: str):
    try:
        result = stat_meal_in_day(date,userId,mealType)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])
        return {
            "message": "Find successful",
            "statMeals": result["statMeals"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
            
@app.delete("/delete_stat_exercise")
async def delete_stat_exercise_api(id: int):
    try:
        result = delete_exercise_of_user(id)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])
        return {
            "message": "Delete successful"
        }
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    
@app.delete("/delete_stat_drink")
async def delete_stat_drink_api(id: int):
    try:
        result = delete_drink_of_user(id)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])
        return {
            "message": "Delete successful"
        }
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    
@app.delete("/delete_stat_meal")
async def delete_stat_meal_api(id: int):
    try:
        result = delete_meal_of_user(id)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])
        return {
            "message": "Delete successful"
        }
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/info_dish")
async def get_dish_api(id: int):
    try:
        result = get_dish_by_id(id)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])
        return {
            "message": "Find successful",
            "dish": result["dish"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.patch("/update_dish")
async def update_dish_api(id: int, dish: DishRequest):
    try:
        result = update_dish_in_db(id,dish)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])
        return {
            "message": "Update dish successful",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/required_index")
async def get_required_index_by_id_api(id: int):
    try:
        result = get_required_index_by_id(id)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])
        return {
            "message": "Find successful",
            "requiredIndex": result["requiredIndex"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/total_water")
async def get_total_water_api(date: date, userId: int):
    try:
        result = get_total_water(date,userId)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])
        return {
            "message": "Find successful",
            "totalWater": result["totalWater"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}") 

@app.get("/info_ingredient")
async def get_ingredient_api(id: int):
    try:
        result = get_ingredient_by_id(id)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])
        return {
            "message": "Find successful",
            "ingredient": result["ingredient"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")  
    
@app.post("/add_ingredient")
async def add_ingredient_api(data: IngredientRequest):
    if not data.name: 
        raise HTTPException(status_code=400, detail="Missing name ingredient")

    result = insert_ingredient_to_db(data)

    if not result['success']:
        raise HTTPException(status_code=401, detail=result['error'])

    return {"message": "Create ingredient successful"}

@app.patch("/update_ingredient")
async def update_ingredient_api(data: UpdateIngredientRequest):
    try:
        result = update_ingredient_in_db(data)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])
        return {
            "message": "Update ingredient successful",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/get_user_info")
async def get_user_info_api(id: int):
    try:
        result = get_user_info_by_id(id)

        if not result['success']:
            raise HTTPException(status_code=404, detail=result['error'])

        return {
            "message": "Find successful",
            "userInfo": result["userInfo"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.post("/add_notification")
async def add_notification_api(notification: AddNotificationRequest):
    result = insert_notification_to_db(notification)

    if not result['success']:
        raise HTTPException(status_code=401, detail=result['error'])

    return {"message": "Create notification successful"}
 
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
