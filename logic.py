from db import get_connection
from datetime import date
from models import UserInfoRequest,DishRequest,AddMealRequest,AddDrinkRequest,AddExerciseRequest,IngredientRequest,UpdateIngredientRequest,AddNotificationRequest
from extra_logic import calculate_dish_hashtags,build_required_index_data
import firebase_admin
from firebase_admin import credentials, messaging

cred = credentials.Certificate("C:/Users/Admin/Desktop/MyHealth/foodai-2a6fb-firebase-adminsdk-fbsvc-e4960dc1b2.json")
firebase_admin.initialize_app(cred)

admin_token = "eUTg2c9UR5uoWyy6GgZt-P:APA91bHr1pCZmvtSm1Nahw6tHPm-2LA5foMGCuu-heYUrx8Dl4PpEE4GtmV60pN0GtHtbLCLcOr13Vpa5dVU9sPOEdI0zIUIU45k8nSniFTFoIFQNAc5QPc"


def insert_userinfo_to_db(user: UserInfoRequest):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        insert_sql = """
            INSERT INTO UserInfo 
            (fullName, gender, age, height, weight, weightTarget, dateTarget, Accountid, ActivityLevelid, Dietid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """
        values = (
            user.fullName,
            user.gender,
            user.age,
            user.height,
            user.weight,
            user.weightTarget,
            user.dateTarget,
            user.Accountid,
            user.ActivityLevelid,
            user.Dietid,
        )
        cur.execute(insert_sql, values)
        new_id = cur.fetchone()[0]

        if user.LimitFoodid:
            limit_food_data = [(new_id, lf_id) for lf_id in user.LimitFoodid]
            cur.executemany(
                "INSERT INTO LimitFoodUser (UserInfoid, LimitFoodid) VALUES (%s, %s)",
                limit_food_data
            )
        if user.HealthStatusid:
            health_status_data = [(new_id, hs_id) for hs_id in user.HealthStatusid]
            cur.executemany(
                "INSERT INTO HealthStatusUser (UserInfoid, HealthStatusid) VALUES (%s, %s)",
                health_status_data
            )

        update_sql = """
            UPDATE Account
            SET isCollectionInfo = 1
            WHERE id = %s
        """
        cur.execute(update_sql, (user.Accountid,))

        index_data = build_required_index_data(user)

        required_sql = """
            INSERT INTO RequiredIndex 
            (UserInfoid, bmr, tdee, targetCalories, water, protein, totalFat,
            saturatedFat, monounSaturatedFat, polyunSaturatedFat, transFat,
            carbohydrate, carbs, sugar, fiber, cholesterol, vitaminA, vitaminD,
            vitaminC, vitaminB6, vitaminB12, vitaminE, vitaminK, choline, canxi,
            fe, magie, photpho, kali, natri, zn, caffeine, alcohol)
            VALUES (%(UserInfoid)s, %(bmr)s, %(tdee)s, %(targetCalories)s, %(water)s,
                    %(protein)s, %(totalFat)s, %(saturatedFat)s, %(monounSaturatedFat)s,
                    %(polyunSaturatedFat)s, %(transFat)s, %(carbohydrate)s, %(carbs)s,
                    %(sugar)s, %(fiber)s, %(cholesterol)s, %(vitaminA)s, %(vitaminD)s,
                    %(vitaminC)s, %(vitaminB6)s, %(vitaminB12)s, %(vitaminE)s,
                    %(vitaminK)s, %(choline)s, %(canxi)s, %(fe)s, %(magie)s,
                    %(photpho)s, %(kali)s, %(natri)s, %(zn)s, %(caffeine)s,
                    %(alcohol)s)
            RETURNING id;
        """
        required_values = index_data["requiredIndex"]
        required_values["UserInfoid"] = new_id

        cur.execute(required_sql, required_values)
        required_id = cur.fetchone()[0]

        hashtags = []
        for tag in index_data["banHashtags"]:
            hashtags.append((required_id, tag, "ban"))
        for tag in index_data["encourageHashtags"]:
            hashtags.append((required_id, tag, "encourage"))
        for tag in index_data["limitHashtags"]:
            hashtags.append((required_id, tag, "limit"))

        if hashtags:
            cur.executemany(
                "INSERT INTO ImportantHashtag (RequiredIndexid, Hashtagid, typeRequest) VALUES (%s, %s, %s)",
                hashtags
            )

        conn.commit()
        return {"success": True, "id": new_id}

    except Exception as e:
        if conn:
            conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def update_userinfo_in_db(id: int, user: UserInfoRequest):
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        update_sql = """
            UPDATE UserInfo
            SET height = %s,
                weight = %s,
                weightTarget = %s,
                dateTarget = %s,
                ActivityLevelid = %s,
                Dietid = %s
            WHERE id = %s
        """
        cur.execute(update_sql, (
            user.height,
            user.weight,
            user.weightTarget,
            user.dateTarget,
            user.ActivityLevelid,
            user.Dietid,
            id
        ))

        cur.execute("DELETE FROM LimitFoodUser WHERE UserInfoid = %s", (id,))
        if user.LimitFoodid:
            limit_food_data = [(id, lf_id) for lf_id in user.LimitFoodid]
            cur.executemany(
                "INSERT INTO LimitFoodUser (UserInfoid, LimitFoodid) VALUES (%s, %s)",
                limit_food_data
            )

        cur.execute("DELETE FROM HealthStatusUser WHERE UserInfoid = %s", (id,))
        if user.HealthStatusid:
            health_status_data = [(id, hs_id) for hs_id in user.HealthStatusid]
            cur.executemany(
                "INSERT INTO HealthStatusUser (UserInfoid, HealthStatusid) VALUES (%s, %s)",
                health_status_data
            )

        index_data = build_required_index_data(user)

        cur.execute("SELECT id FROM RequiredIndex WHERE UserInfoid = %s", (id,))
        row = cur.fetchone()

        if row:
            required_id = row[0]
            update_req_sql = """
                UPDATE RequiredIndex
                SET bmr = %(bmr)s, tdee = %(tdee)s, targetCalories = %(targetCalories)s,
                    water = %(water)s, protein = %(protein)s, totalFat = %(totalFat)s,
                    saturatedFat = %(saturatedFat)s, monounSaturatedFat = %(monounSaturatedFat)s,
                    polyunSaturatedFat = %(polyunSaturatedFat)s, transFat = %(transFat)s,
                    carbohydrate = %(carbohydrate)s, carbs = %(carbs)s, sugar = %(sugar)s,
                    fiber = %(fiber)s, cholesterol = %(cholesterol)s, vitaminA = %(vitaminA)s,
                    vitaminD = %(vitaminD)s, vitaminC = %(vitaminC)s, vitaminB6 = %(vitaminB6)s,
                    vitaminB12 = %(vitaminB12)s, vitaminE = %(vitaminE)s, vitaminK = %(vitaminK)s,
                    choline = %(choline)s, canxi = %(canxi)s, fe = %(fe)s, magie = %(magie)s,
                    photpho = %(photpho)s, kali = %(kali)s, natri = %(natri)s, zn = %(zn)s,
                    caffeine = %(caffeine)s, alcohol = %(alcohol)s
                WHERE id = %(required_id)s
            """
            required_values = index_data["requiredIndex"]
            required_values["required_id"] = required_id
            cur.execute(update_req_sql, required_values)
        else:
            required_sql = """
                INSERT INTO RequiredIndex (UserInfoid, bmr, tdee, targetCalories, water, protein, totalFat,
                saturatedFat, monounSaturatedFat, polyunSaturatedFat, transFat, carbohydrate, carbs, sugar,
                fiber, cholesterol, vitaminA, vitaminD, vitaminC, vitaminB6, vitaminB12, vitaminE,
                vitaminK, choline, canxi, fe, magie, photpho, kali, natri, zn, caffeine, alcohol)
                VALUES (%(UserInfoid)s, %(bmr)s, %(tdee)s, %(targetCalories)s, %(water)s, %(protein)s,
                %(totalFat)s, %(saturatedFat)s, %(monounSaturatedFat)s, %(polyunSaturatedFat)s,
                %(transFat)s, %(carbohydrate)s, %(carbs)s, %(sugar)s, %(fiber)s, %(cholesterol)s,
                %(vitaminA)s, %(vitaminD)s, %(vitaminC)s, %(vitaminB6)s, %(vitaminB12)s, %(vitaminE)s,
                %(vitaminK)s, %(choline)s, %(canxi)s, %(fe)s, %(magie)s, %(photpho)s, %(kali)s,
                %(natri)s, %(zn)s, %(caffeine)s, %(alcohol)s)
                RETURNING id;
            """
            required_values = index_data["requiredIndex"]
            required_values["UserInfoid"] = id
            cur.execute(required_sql, required_values)
            required_id = cur.fetchone()[0]

        cur.execute("DELETE FROM ImportantHashtag WHERE RequiredIndexid = %s", (required_id,))
        hashtags = []
        for tag in index_data["banHashtags"]:
            hashtags.append((required_id, tag, "ban"))
        for tag in index_data["encourageHashtags"]:
            hashtags.append((required_id, tag, "encourage"))
        for tag in index_data["limitHashtags"]:
            hashtags.append((required_id, tag, "limit"))

        if hashtags:
            cur.executemany(
                "INSERT INTO ImportantHashtag (RequiredIndexid, Hashtagid, typeRequest) VALUES (%s, %s, %s)",
                hashtags
            )
        conn.commit()
        return {"success": True, "id": id}

    except Exception as e:
        if conn:
            conn.rollback()
        return {"success": False, "error": str(e)}

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def register_account(username: str, password: str):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT id FROM Account WHERE userName = %s", (username,))
        if cur.fetchone() is not None:
            return {"success": False, "error": "Username already exists"}

        insert_sql = """
            INSERT INTO Account (userName, passWord, isCollectionInfo)
            VALUES (%s, %s, %s)
            RETURNING id;
        """
        cur.execute(insert_sql, (username, password, 0))
        new_id = cur.fetchone()[0]
        conn.commit()

        return {"success": True, "id": new_id}

    except Exception as e:
        if conn:
            conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def login_account(username: str, password: str):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT id, passWord, isCollectionInfo
            FROM Account
            WHERE userName = %s
        """
        cur.execute(sql, (username,))
        row = cur.fetchone()

        if row is None:
            return {"success": False, "error": "Username not found"}

        account_id, db_password, is_collection_info = row
        if db_password != password:
            return {"success": False, "error": "Incorrect password"}

        result = {
            "success": True,
            "account": {
                "id": account_id,
                "userName": username,
                "isCollectionInfo": is_collection_info
            }
        }

        if is_collection_info == 1:
            cur.execute(
                "SELECT id FROM UserInfo WHERE Accountid = %s LIMIT 1",
                (account_id,)
            )
            user_info_row = cur.fetchone()
            if user_info_row:
                result["idUserInfo"] = user_info_row[0]

        return result

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def similar_food(keyword: str):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT name, similarity(name, %s) AS sml
            FROM ingredient
            WHERE name ILIKE %s OR name %% %s
            ORDER BY sml DESC
            LIMIT 10;
        """

        cur.execute(sql, (keyword,f"%{keyword}%",keyword,))
        rows = cur.fetchall()

        if not rows:
            return {"success": False, "error": "Similar food not found"}

        similar_list = [r[0] for r in rows]

        result = {
            "success": True,
            "similar": similar_list
        }
        return result

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def find_food(keyword: str):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        if not keyword:
            sql = """
            SELECT id,name,thumbnail,kcal,baseUnit,isConfirm
            FROM ingredient
            ORDER BY id ASC;
            """
            cur.execute(sql)
        else:
            sql = """
            SELECT id,name,thumbnail,kcal,baseUnit,isConfirm
            FROM ingredient
            WHERE name ILIKE %s OR name %% %s
            ORDER BY similarity(name, %s) DESC;
            """
            cur.execute(sql, (f"%{keyword}%",keyword,keyword,))
        rows = cur.fetchall()

        if not rows:
            return {"success": False, "error": "Similar food not found"}

        similar_list = [
            {"id": r[0], "name": r[1], "thumbnail": r[2], "kcal": r[3],"baseUnit": r[4],"isConfirm": r[5]}
            for r in rows
        ]

        result = {
            "success": True,
            "similar": similar_list
        }
        return result

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def similar_dish(keyword: str):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT name, similarity(name, %s) AS sml
            FROM dish
            WHERE name ILIKE %s OR name %% %s
            ORDER BY sml DESC
            LIMIT 10;
        """

        cur.execute(sql, (keyword,f"%{keyword}%",keyword,))
        rows = cur.fetchall()

        if not rows:
            return {"success": False, "error": "Similar food not found"}

        similar_list = [r[0] for r in rows]

        result = {
            "success": True,
            "similar": similar_list
        }
        return result

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def find_dish(keyword: str):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        if not keyword:
            sql = """
            SELECT d.id,
                   d.name,
                   d.thumbnail,
                   d.isconfirm,
                   COALESCE(SUM(iid.weight * ing.gramperunit), 0) AS totalgram,
                   COALESCE(SUM((iid.weight * ing.gramperunit / 100.0) * ing.kcal), 0) AS totalkcal
            FROM dish d
            LEFT JOIN ingredientindish iid ON d.id = iid.dishid
            LEFT JOIN ingredient ing ON iid.ingredientid = ing.id
            GROUP BY d.id, d.name, d.thumbnail, d.isconfirm
            ORDER BY d.id ASC;
            """
            cur.execute(sql)
        else:
            sql = """
            SELECT d.id,
                   d.name,
                   d.thumbnail,
                   d.isconfirm,
                   COALESCE(SUM(iid.weight * ing.gramperunit), 0) AS totalgram,
                   COALESCE(SUM((iid.weight * ing.gramperunit / 100.0) * ing.kcal), 0) AS totalkcal
            FROM dish d
            LEFT JOIN ingredientindish iid ON d.id = iid.dishid
            LEFT JOIN ingredient ing ON iid.ingredientid = ing.id
            WHERE d.name ILIKE %s OR d.name %% %s
            GROUP BY d.id, d.name, d.thumbnail, d.isconfirm
            ORDER BY similarity(d.name, %s) DESC;
            """
            cur.execute(sql, (f"%{keyword}%", keyword, keyword))

        rows = cur.fetchall()

        if not rows:
            return {"success": False, "error": "Similar food not found"}

        similar_list = [
            {
                "id": r[0],
                "name": r[1],
                "thumbnail": r[2],
                "isConfirm": r[3],
                "totalGram": float(r[4]),
                "totalKcal": float(r[5]),
            }
            for r in rows
        ]

        result = {
            "success": True,
            "similar": similar_list
        }
        return result

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def insert_dish_to_db(dish: DishRequest):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        insert_sql = """
            INSERT INTO Dish
            (name, thumbnail, isConfirm, description, preparationSteps, cookingSteps)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
        """
        values = (
            dish.name,
            dish.thumbnail,
            dish.isConfirm,
            dish.description,
            dish.preparationSteps,
            dish.cookingSteps,
        )
        cur.execute(insert_sql, values)
        new_dish_id = cur.fetchone()[0]

        if dish.ingredients:
            dish_ingredient_data = [
                (ing.ingredientId, new_dish_id, ing.weight)
                for ing in dish.ingredients
            ]
            cur.executemany(
                """
                INSERT INTO IngredientInDish (Ingredientid, Dishid, weight)
                VALUES (%s, %s, %s)
                """,
                dish_ingredient_data
            )

        all_hashtags = []

        if dish.hashtagId:
            all_hashtags.extend(dish.hashtagId)

        auto_hashtags = calculate_dish_hashtags(new_dish_id)

        if auto_hashtags:
            all_hashtags.extend(auto_hashtags)

        if all_hashtags:
            hashtag_data = [
                (hashtag_id, new_dish_id) for hashtag_id in all_hashtags
            ]
            cur.executemany(
                """
                INSERT INTO HashtagOfDish (Hashtagid, Dishid)
                VALUES (%s, %s)
                """,
                hashtag_data
            )

        conn.commit()
        return {"success": True, "id": new_dish_id}

    except Exception as e:
        if conn:
            conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def login_admin(username: str, password: str):
    try:
        if username == "AdminMyHealth" and password == "LapAn123" :
            return {"success": True}
        else:
            return {"success": False, "error": "Username not found"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def insert_meal_to_db(meal: AddMealRequest):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        insert_sql = """
                INSERT INTO MealOfUser (time, mealType, weight, UserInfoId, DishId)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id;
            """
        values = (meal.date, meal.mealType, meal.weight, meal.userId, meal.dishId)

        cur.execute(insert_sql, values)
        new_id = cur.fetchone()[0]
        conn.commit()

        return {"success": True, "id": new_id}

    except Exception as e:
        if conn:
            conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def insert_drink_to_db(drink: AddDrinkRequest):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        insert_sql = """
                INSERT INTO DrinkOfUser (time, amount, UnitDrinkId, UserInfoid)
                VALUES (%s, %s, %s, %s)
                RETURNING id;
            """
        values = (drink.date, drink.amount, drink.unitDrinkId, drink.userId)

        cur.execute(insert_sql, values)
        new_id = cur.fetchone()[0]
        conn.commit()

        return {"success": True, "id": new_id}

    except Exception as e:
        if conn:
            conn.rollback()
        print("Insert error:", e)
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def find_exercise():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT id,nameExercise,detail,thumbnail FROM public.exercise
            ORDER BY id ASC 
            """
        cur.execute(sql)
        rows = cur.fetchall()

        if not rows:
            return {"success": False, "error": "Not found"}

        exercise_list = [
            {"id": r[0], "nameExercise": r[1], "detail": r[2], "thumbnail": r[3]}
            for r in rows
        ]

        result = {
            "success": True,
            "exercises": exercise_list
        }
        return result

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def insert_exercise_to_db(exercise: AddExerciseRequest):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        insert_sql = """
                INSERT INTO ExerciseOfUser (time, minute, levelExerciseId, userInfoId)
                VALUES (
                    %s,
                    %s,
                    (
                        SELECT le.id 
                        FROM LevelExercise le
                        JOIN Exercise e ON le.Exerciseid = e.id
                        WHERE e.id = %s  
                        AND le.level = %s 
                        LIMIT 1
                    ),
                    %s
                )
                RETURNING id;
            """
        values = (exercise.date, exercise.time, exercise.exerciseId, exercise.levelExercise, exercise.userId)

        cur.execute(insert_sql, values)
        new_id = cur.fetchone()[0]
        conn.commit()

        return {"success": True, "id": new_id}

    except Exception as e:
        if conn:
            conn.rollback()
        print("Insert error:", e)
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def total_kcal_exercise(date: date, userId: int):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT COALESCE(SUM(eou.minute * le.kcalPerMin), 0) AS total_kcal
            FROM ExerciseOfUser eou
            JOIN LevelExercise le ON eou.LevelExerciseid = le.id
            WHERE eou.UserInfoid = %s
            AND DATE(eou.time) = %s;
            """
        
        values = (userId,date)
        cur.execute(sql,values)
        total_kcal = cur.fetchone()[0]

        result = {
            "success": True,
            "totalKcal": total_kcal
        }
        return result

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def total_nutri_meal(date: date, userId: int):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT 
            SUM((ing.kcal        / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_kcal,
            SUM((ing.carbs       / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_carbs,
            SUM((ing.sugar       / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_sugar,
            SUM((ing.fiber       / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_fiber,
            SUM((ing.protein     / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_protein,
            SUM((ing.saturatedFat / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_saturatedFat,
            SUM((ing.monounSaturatedFat / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_monounSaturatedFat,
            SUM((ing.polyunSaturatedFat / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_polyunSaturatedFat,
            SUM((ing.transFat    / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_transFat,
            SUM((ing.cholesterol / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_cholesterol,
            SUM((ing.vitaminA    / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_vitaminA,
            SUM((ing.vitaminC    / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_vitaminC,
            SUM((ing.vitaminD    / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_vitaminD,
            SUM((ing.vitaminB6   / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_vitaminB6,
            SUM((ing.vitaminB12  / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_vitaminB12,
            SUM((ing.vitaminE    / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_vitaminE,
            SUM((ing.vitaminK    / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_vitaminK,
            SUM((ing.choline     / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_choline,
            SUM((ing.canxi       / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_canxi,
            SUM((ing.fe          / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_fe,
            SUM((ing.magie       / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_magie,
            SUM((ing.photpho     / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_photpho,
            SUM((ing.kali        / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_kali,
            SUM((ing.natri       / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_natri,
            SUM((ing.zn          / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_zn,
            SUM((ing.water       / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_water,
            SUM((ing.caffeine     / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_cafeine,
            SUM((ing.alcohol     / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS total_alcohol
            FROM MealOfUser mu
            JOIN Dish d ON mu.Dishid = d.id
            JOIN (
            SELECT Dishid, SUM(weight) AS total_weight
            FROM IngredientInDish
            GROUP BY Dishid
            ) d_weight ON d.id = d_weight.Dishid
            JOIN IngredientInDish iid ON d.id = iid.Dishid
            JOIN Ingredient ing ON iid.Ingredientid = ing.id
            WHERE mu.UserInfoid = %s
            AND DATE(mu.time) = %s
            GROUP BY mu.UserInfoid, DATE(mu.time);
            """
        
        values = (userId,date)
        cur.execute(sql,values)
        r = cur.fetchone()

        nutri_meal = {
            "kcal": r[0],
            "totalCarbs": r[1]+r[2]+r[3],
            "carbs": r[1],
            "sugar": r[2],
            "fiber": r[3],
            "protein": r[4],
            "totalFats": r[1]+r[2]+r[3],
            "saturatedFat": r[5],
            "monounSaturatedFat": r[6],
            "polyunSaturatedFat": r[7],
            "transFat": r[8],
            "cholesterol": r[9],
            "vitaminA": r[10],
            "vitaminC": r[11],
            "vitaminD": r[12],
            "vitaminB6": r[13],
            "vitaminB12": r[14],
            "vitaminE": r[15],
            "vitaminK": r[16],
            "choline": r[17],
            "canxi": r[18],
            "fe": r[19],
            "magie": r[20],
            "photpho": r[21],
            "kali": r[22],
            "natri": r[23],
            "zn": r[24],
            "water": r[25],
            "caffeine": r[26],
            "alcohol": r[27],
        }
        result = {
            "success": True,
            "nutriMeal": nutri_meal
        }
        return result

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def stat_drink_in_day(date: date, userId: int):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT 
            d.id,
            d.amount,
            u.baseUnit,
            u.thumbnail,
			d.createdAt
            FROM DrinkOfUser d
            JOIN UnitDrink u ON d.UnitDrinkid = u.id
            WHERE d.UserInfoid = %s
            AND DATE(d.time) = %s
            ORDER BY d.createdAt;
            """
        values = (userId, date)
        cur.execute(sql,values)
        rows = cur.fetchall()

        if not rows:
            return {"success": False, "error": "Not found"}

        stat_drinks = [
            {"id": r[0], "amount": r[1], "baseUnit": r[2], "thumbnail": r[3], "createdAt": r[4]}
            for r in rows
        ]

        result = {
            "success": True,
            "statDrinks": stat_drinks
        }
        return result

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def stat_exercise_in_day(date: date, userId: int):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
                SELECT 
                eu.id,
                e.nameExercise,
                e.thumbnail,
                le.level,
                eu.minute,
                le.kcalPerMin,
                eu.createdAt
                FROM ExerciseOfUser eu
                JOIN LevelExercise le ON eu.LevelExerciseid = le.id
                JOIN Exercise e ON le.Exerciseid = e.id
                WHERE eu.UserInfoid = %s
                AND DATE(eu.time) = %s
                ORDER BY eu.createdAt;
            """
        values = (userId, date)
        cur.execute(sql,values)
        rows = cur.fetchall()

        if not rows:
            return {"success": False, "error": "Not found"}

        stat_exercises = [
            {"id": r[0], "nameExercise": r[1], "thumbnail": r[2], "level": r[3], "minute": r[4], "kcalPerMin": r[5], "createdAt": r[6]}
            for r in rows
        ]

        result = {
            "success": True,
            "statExercises": stat_exercises
        }
        return result

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def stat_meal_in_day(date: date, userId: int, mealType: str):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
                SELECT 
                mu.id,
                d.name,
                d.thumbnail,
                mu.weight,
                d.isConfirm,
                mu.createdAt,
                SUM((ing.kcal / 100.0) * iid.weight * (mu.weight / d_weight.total_weight)) AS kcal
                FROM MealOfUser mu
                JOIN Dish d ON mu.Dishid = d.id
                JOIN (
                SELECT Dishid, SUM(weight) AS total_weight
                FROM IngredientInDish
                GROUP BY Dishid
                ) d_weight ON d.id = d_weight.Dishid
                JOIN IngredientInDish iid ON d.id = iid.Dishid
                JOIN Ingredient ing ON iid.Ingredientid = ing.id
                WHERE mu.UserInfoid = %s
                AND mu.mealType = %s
                AND DATE(mu.time) = %s
                GROUP BY mu.id, d.name, d.thumbnail, mu.weight, d.isConfirm, mu.createdAt
                ORDER BY mu.createdAt;
            """
        values = (userId, mealType, date)
        cur.execute(sql,values)
        rows = cur.fetchall()

        if not rows:
            return {"success": False, "error": "Not found"}

        stat_meals = [
            {"id": r[0], "name": r[1], "thumbnail": r[2], "weight": r[3], "isConfirm": r[4], "createdAt": r[5], "kcal": r[6]}
            for r in rows
        ]

        result = {
            "success": True,
            "statMeals": stat_meals
        }
        return result

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def delete_exercise_of_user(id: int):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            DELETE FROM public.exerciseofuser
            WHERE id = %s
            RETURNING id;
        """
        value=(id,)
        cur.execute(sql,value)
        deleted_row = cur.fetchone()
        conn.commit()

        if not deleted_row:
            return {"success": False, "error": f"ExerciseOfUser with id {id} not found"}

        result = {
            "success": True,
            "deletedId": deleted_row[0]
        }
        return result

    except Exception as e:
        if conn:
            conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def delete_drink_of_user(id: int):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            DELETE FROM public.drinkofuser
            WHERE id = %s
            RETURNING id;
        """
        value=(id,)
        cur.execute(sql,value)
        deleted_row = cur.fetchone()
        conn.commit()

        if not deleted_row:
            return {"success": False, "error": f"DrinkOfUser with id {id} not found"}

        result = {
            "success": True,
            "deletedId": deleted_row[0]
        }
        return result

    except Exception as e:
        if conn:
            conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def delete_meal_of_user(id: int):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            DELETE FROM public.mealofuser
            WHERE id = %s
            RETURNING id;
        """
        value=(id,)
        cur.execute(sql,value)
        deleted_row = cur.fetchone()
        conn.commit()

        if not deleted_row:
            return {"success": False, "error": f"MealOfUser with id {id} not found"}

        result = {
            "success": True,
            "deletedId": deleted_row[0]
        }
        return result

    except Exception as e:
        if conn:
            conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def get_dish_by_id(id: int):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        value=(id,)

        sql1 = """
            SELECT id,name,thumbnail,isConfirm,description,preparationSteps,cookingSteps
            FROM Dish
            WHERE id = %s; 
            """
        cur.execute(sql1,value)
        r1 = cur.fetchone()

        if not r1:
            return {"success": False, "error": "Dish not found"}

        dish = {"id": r1[0], "name": r1[1], "thumbnail": r1[2], "isConfirm": r1[3], "description": r1[4], "preparationSteps": r1[5], "cookingSteps": r1[6]}

        sql2 = """
            SELECT iid.IngredientId,i.name,iid.weight,i.baseUnit,i.thumbnail
            FROM IngredientInDish iid
            JOIN Ingredient i ON iid.IngredientId = i.id
            WHERE iid.DishId = %s;
            """
        cur.execute(sql2,value)
        r2 = cur.fetchall()

        ingredients = [
            {
                "ingredientId": r[0],
                "name": r[1],
                "weight": r[2],
                "unit": r[3],
                "thumbnail": r[4]
            }
            for r in r2
        ]
        dish["ingredients"] = ingredients

        sql3 = """
            SELECT h.id,h.title
            FROM HashtagOfDish hd
            JOIN Hashtag h ON hd.HashtagId = h.id
            WHERE hd.DishId = %s;
            """
        cur.execute(sql3,value)
        r3 = cur.fetchall()
        hashtags = [{"id": r[0], "title": r[1]} for r in r3]
        dish["hashtags"] = hashtags

        result = {
            "success": True,
            "dish": dish
        }
        return result

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def update_dish_in_db(id: int, dish: DishRequest):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql1 = """
            UPDATE Dish
            SET name = %s,
                thumbnail = %s,
                isConfirm = %s,
                description = %s,
                preparationSteps = %s,
                cookingSteps = %s
            WHERE id = %s;
        """
        value1 = (
            dish.name,
            dish.thumbnail,
            dish.isConfirm,
            dish.description,
            dish.preparationSteps,
            dish.cookingSteps,
            id
        )
        cur.execute(sql1, value1)

        if cur.rowcount == 0:
            return {"success": False, "error": "Dish not found"}

        sql2 = "DELETE FROM IngredientInDish WHERE DishId = %s;"
        value2 = (id,)
        cur.execute(sql2, value2)

        if dish.ingredients:
            sql2_insert = """
                INSERT INTO IngredientInDish (Dishid, Ingredientid, weight)
                VALUES (%s, %s, %s)
            """
            data_ing = [
                (id, ing.ingredientId, ing.weight)
                for ing in dish.ingredients
            ]
            cur.executemany(sql2_insert, data_ing)

        sql3 = "DELETE FROM HashtagOfDish WHERE DishId = %s;"
        value3 = (id,)
        cur.execute(sql3, value3)

        all_hashtags = []

        if dish.hashtagId:
            all_hashtags.extend(dish.hashtagId)

        auto_hashtags = calculate_dish_hashtags(id)
        if auto_hashtags:
            all_hashtags.extend(auto_hashtags)

        if all_hashtags:
            sql3_insert = """
                INSERT INTO HashtagOfDish (Hashtagid, Dishid)
                VALUES (%s, %s)
            """
            data_hash = [(h_id, id) for h_id in all_hashtags]
            cur.executemany(sql3_insert, data_hash)

        conn.commit()
        return {"success": True}

    except Exception as e:
        if conn:
            conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def get_required_index_by_id(id: int):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        value = (id,)

        sql = """
            SELECT *
            FROM requiredindex
            WHERE id = %s;
        """
        cur.execute(sql, value)
        r = cur.fetchone()

        if not r:
            return {"success": False, "error": "RequiredIndex not found"}

        required_index = {
            "id": r[0],
            "userinfoid": r[1],
            "bmr": r[2],
            "tdee": r[3],
            "targetcalories": r[4],
            "water": r[5],
            "protein": r[6],
            "totalfat": r[7],
            "saturatedfat": r[8],
            "monounsaturatedfat": r[9],
            "polyunsaturatedfat": r[10],
            "transfat": r[11],
            "carbohydrate": r[12],
            "carbs": r[13],
            "sugar": r[14],
            "fiber": r[15],
            "cholesterol": r[16],
            "vitamina": r[17],
            "vitamind": r[18],
            "vitaminc": r[19],
            "vitaminb6": r[20],
            "vitaminb12": r[21],
            "vitamine": r[22],
            "vitamink": r[23],
            "choline": r[24],
            "canxi": r[25],
            "fe": r[26],
            "magie": r[27],
            "photpho": r[28],
            "kali": r[29],
            "natri": r[30],
            "zn": r[31],
            "caffeine": r[32],
            "alcohol": r[33],
        }

        return {"success": True, "requiredIndex": required_index}

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def get_total_water(date: date, userId: int):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT 
            SUM(d.amount * ud.mlPerUnit) AS total_ml
            FROM DrinkOfUser d
            JOIN UnitDrink ud ON d.UnitDrinkid = ud.id
            JOIN UserInfo u ON d.UserInfoid = u.id
            WHERE u.id = %s
            AND DATE(d.time) = %s
            GROUP BY u.id;
            """
        values = (userId,date)
        cur.execute(sql,values)
        total_water = cur.fetchone()[0]

        result = {
            "success": True,
            "totalWater": total_water
        }
        return result

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def get_ingredient_by_id(id: int):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT *
            FROM public.ingredient
            WHERE id = %s;
        """
        values = (id,)
        cur.execute(sql, values)
        row = cur.fetchone()

        if not row:
            return {"success": False, "error": "Ingredient not found"}

        ingredient = {
            "id": row[0],
            "name": row[1],
            "thumbnail": row[2],
            "baseUnit": row[3],
            "gramPerUnit": row[4],
            "isConfirm": row[5],
            "kcal": row[6],
            "carbs": row[7],
            "sugar": row[8],
            "fiber": row[9],
            "protein": row[10],
            "saturatedFat": row[11],
            "monounSaturatedFat": row[12],
            "polyunSaturatedFat": row[13],
            "transFat": row[14],
            "cholesterol": row[15],
            "vitaminA": row[16],
            "vitaminD": row[17],
            "vitaminC": row[18],
            "vitaminB6": row[19],
            "vitaminB12": row[20],
            "vitaminE": row[21],
            "vitaminK": row[22],
            "choline": row[23],
            "canxi": row[24],
            "fe": row[25],
            "magie": row[26],
            "photpho": row[27],
            "kali": row[28],
            "natri": row[29],
            "zn": row[30],
            "water": row[31],
            "caffeine": row[32],
            "alcohol": row[33],
        }

        return {"success": True, "ingredient": ingredient}

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def insert_ingredient_to_db(ingredient: IngredientRequest):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        insert_sql = """
            INSERT INTO Ingredient (
                name, thumbnail, baseUnit, gramPerUnit, isConfirm, kcal, carbs, sugar, fiber, protein,
                saturatedFat, monounSaturatedFat, polyunSaturatedFat, transFat, cholesterol,
                vitaminA, vitaminD, vitaminC, vitaminB6, vitaminB12, vitaminE, vitaminK,
                choline, canxi, fe, magie, photpho, kali, natri, zn, water, caffeine, alcohol
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING id;
        """

        values = (
            ingredient.name,
            ingredient.thumbnail,
            ingredient.baseUnit,
            ingredient.gramPerUnit,
            ingredient.isConfirm,
            ingredient.kcal,
            ingredient.carbs,
            ingredient.sugar,
            ingredient.fiber,
            ingredient.protein,
            ingredient.saturatedFat,
            ingredient.monounSaturatedFat,
            ingredient.polyunSaturatedFat,
            ingredient.transFat,
            ingredient.cholesterol,
            ingredient.vitaminA,
            ingredient.vitaminD,
            ingredient.vitaminC,
            ingredient.vitaminB6,
            ingredient.vitaminB12,
            ingredient.vitaminE,
            ingredient.vitaminK,
            ingredient.choline,
            ingredient.canxi,
            ingredient.fe,
            ingredient.magie,
            ingredient.photpho,
            ingredient.kali,
            ingredient.natri,
            ingredient.zn,
            ingredient.water,
            ingredient.caffeine,
            ingredient.alcohol
        )

        cur.execute(insert_sql, values)
        new_id = cur.fetchone()[0]
        conn.commit()

        return {"success": True, "id": new_id}

    except Exception as e:
        if conn:
            conn.rollback()
        print("Insert error:", e)
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def update_ingredient_in_db(ingredient: UpdateIngredientRequest):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        update_sql = """
            UPDATE Ingredient
            SET
                name = %s,
                thumbnail = %s,
                baseUnit = %s,
                gramPerUnit = %s,
                isConfirm = %s,
                kcal = %s,
                carbs = %s,
                sugar = %s,
                fiber = %s,
                protein = %s,
                saturatedFat = %s,
                monounSaturatedFat = %s,
                polyunSaturatedFat = %s,
                transFat = %s,
                cholesterol = %s,
                vitaminA = %s,
                vitaminD = %s,
                vitaminC = %s,
                vitaminB6 = %s,
                vitaminB12 = %s,
                vitaminE = %s,
                vitaminK = %s,
                choline = %s,
                canxi = %s,
                fe = %s,
                magie = %s,
                photpho = %s,
                kali = %s,
                natri = %s,
                zn = %s,
                water = %s,
                caffeine = %s,
                alcohol = %s
            WHERE id = %s;
        """

        values = (
            ingredient.name,
            ingredient.thumbnail,
            ingredient.baseUnit,
            ingredient.gramPerUnit,
            ingredient.isConfirm,
            ingredient.kcal,
            ingredient.carbs,
            ingredient.sugar,
            ingredient.fiber,
            ingredient.protein,
            ingredient.saturatedFat,
            ingredient.monounSaturatedFat,
            ingredient.polyunSaturatedFat,
            ingredient.transFat,
            ingredient.cholesterol,
            ingredient.vitaminA,
            ingredient.vitaminD,
            ingredient.vitaminC,
            ingredient.vitaminB6,
            ingredient.vitaminB12,
            ingredient.vitaminE,
            ingredient.vitaminK,
            ingredient.choline,
            ingredient.canxi,
            ingredient.fe,
            ingredient.magie,
            ingredient.photpho,
            ingredient.kali,
            ingredient.natri,
            ingredient.zn,
            ingredient.water,
            ingredient.caffeine,
            ingredient.alcohol,
            ingredient.id
        )

        cur.execute(update_sql, values)
        conn.commit()

        return {"success": True}

    except Exception as e:
        if conn:
            conn.rollback()
        print("Update error:", e)
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def get_user_info_by_id(id: int):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT
              u.id,
              u.fullname,
              u.age,
              u.height,
              u.weight,
              al.title AS activityLevel,
              STRING_AGG(DISTINCT lf.title, ', ') FILTER (WHERE lf.title IS NOT NULL) AS limitFood,
              STRING_AGG(DISTINCT hs.title, ', ') FILTER (WHERE hs.title IS NOT NULL) AS healthStatus,
              d.title AS diet,
              ri.bmr,
              ri.tdee,
              u.gender
            FROM public.userinfo u
            LEFT JOIN public.activitylevel al ON u.activitylevelid = al.id
            LEFT JOIN public.diet d ON u.dietid = d.id
            LEFT JOIN public.requiredindex ri ON ri.userinfoid = u.id
            LEFT JOIN public.limitfooduser lfu ON lfu.userinfoid = u.id
            LEFT JOIN public.limitfood lf ON lfu.limitfoodid = lf.id
            LEFT JOIN public.healthstatususer hsu ON hsu.userinfoid = u.id
            LEFT JOIN public.healthstatus hs ON hsu.healthstatusid = hs.id
            WHERE u.id = %s
            GROUP BY
              u.id, u.fullname, u.age, u.height, u.weight,
              al.title, d.title, ri.bmr, ri.tdee, u.gender;
        """
        values = (id,)
        cur.execute(sql, values)
        row = cur.fetchone()

        if not row:
            return {"success": False, "error": "User not found"}

        user_info = {
            "id": row[0],
            "fullname": row[1],
            "age": row[2],
            "height": row[3],
            "weight": row[4],
            "activityLevel": row[5],
            "limitFood": row[6],
            "healthStatus": row[7],
            "diet": row[8],
            "bmr": row[9],
            "tdee": row[10],
            "gender": row[11],
        }

        return {"success": True, "userInfo": user_info}

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()

def insert_notification_to_db(notification: AddNotificationRequest):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        if notification.receiverId == 0:
            receiver_token = admin_token
        else:
            cur.execute("SELECT token FROM public.userinfo WHERE id = %s;", (notification.receiverId,))
            token_row = cur.fetchone()
            if not token_row or not token_row[0]:
                return {"success": False, "error": "Receiver not found or has no token"}
            receiver_token = token_row[0]

        insert_sql = """
                    INSERT INTO notification (senderId, receiverId, type, content, relatedId)
                    VALUES
                    (%s, %s, %s, %s, %s)
                    RETURNING id;
                """
        values = (notification.senderId, notification.receiverId, notification.type, notification.content, notification.relatedId)

        cur.execute(insert_sql, values)
        new_id = cur.fetchone()[0]
        conn.commit()

        type_titles = {
            "ADD_DISH": "Người dùng thêm món ăn mới!",
            "ADD_INGREDIENT": "Người dùng thêm nguyên liệu mới!",
            "FEEDBACK": "Người dùng góp ý tới hệ thống!",
            "RESPOND": "Quản trị viên phản hồi tới bạn!"
        }
        title = type_titles.get(notification.type, type_titles["RESPOND"])

        message = messaging.Message(
            notification=messaging.Notification(
                title=title,
                body=notification.content,
            ),
        token=receiver_token,
        )
        messaging.send(message)

        return {"success": True, "id": new_id}

    except Exception as e:
        if conn:
            conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            cur.close()
            conn.close()
