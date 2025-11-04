from db import get_connection
from datetime import date,datetime
from models import UserInfoRequest

def calculate_dish_hashtags(dish_id: int) -> list[int]:
    conn = None
    hashtags = []
    try:
        conn = get_connection()
        cur = conn.cursor()

        query = """
            SELECT 
                SUM(i.kcal * iid.weight / 100) / SUM(iid.weight) * 100 AS kcal,
                SUM(i.carbs * iid.weight / 100) / SUM(iid.weight) * 100 AS carbs,
                SUM(i.sugar * iid.weight / 100) / SUM(iid.weight) * 100 AS sugar,
                SUM(i.fiber * iid.weight / 100) / SUM(iid.weight) * 100 AS fiber,
                SUM(i.protein * iid.weight / 100) / SUM(iid.weight) * 100 AS protein,
                SUM(i.saturatedFat * iid.weight / 100) / SUM(iid.weight) * 100 AS saturatedFat,
                SUM(i.monounSaturatedFat * iid.weight / 100) / SUM(iid.weight) * 100 AS monounSaturatedFat,
                SUM(i.polyunSaturatedFat * iid.weight / 100) / SUM(iid.weight) * 100 AS polyunSaturatedFat,
                SUM(i.transFat * iid.weight / 100) / SUM(iid.weight) * 100 AS transFat,
                SUM(i.cholesterol * iid.weight / 100) / SUM(iid.weight) * 100 AS cholesterol,
                SUM(i.vitaminA * iid.weight / 100) / SUM(iid.weight) * 100 AS vitaminA,
                SUM(i.vitaminC * iid.weight / 100) / SUM(iid.weight) * 100 AS vitaminC,
                SUM(i.vitaminD * iid.weight / 100) / SUM(iid.weight) * 100 AS vitaminD,
                SUM(i.vitaminB6 * iid.weight / 100) / SUM(iid.weight) * 100 AS vitaminB6,
                SUM(i.vitaminB12 * iid.weight / 100) / SUM(iid.weight) * 100 AS vitaminB12,
                SUM(i.vitaminE * iid.weight / 100) / SUM(iid.weight) * 100 AS vitaminE,
                SUM(i.vitaminK * iid.weight / 100) / SUM(iid.weight) * 100 AS vitaminK,
                SUM(i.choline * iid.weight / 100) / SUM(iid.weight) * 100 AS choline,
                SUM(i.canxi * iid.weight / 100) / SUM(iid.weight) * 100 AS canxi,
                SUM(i.fe * iid.weight / 100) / SUM(iid.weight) * 100 AS fe,
                SUM(i.magie * iid.weight / 100) / SUM(iid.weight) * 100 AS magie,
                SUM(i.photpho * iid.weight / 100) / SUM(iid.weight) * 100 AS photpho,
                SUM(i.kali * iid.weight / 100) / SUM(iid.weight) * 100 AS kali,
                SUM(i.natri * iid.weight / 100) / SUM(iid.weight) * 100 AS natri,
                SUM(i.zn * iid.weight / 100) / SUM(iid.weight) * 100 AS zn,
                SUM(i.caffeine * iid.weight / 100) / SUM(iid.weight) * 100 AS caffeine,
                SUM(i.alcohol * iid.weight / 100) / SUM(iid.weight) * 100 AS alcohol
            FROM Dish d
            JOIN IngredientInDish iid ON d.id = iid.Dishid
            JOIN Ingredient i ON iid.Ingredientid = i.id
            WHERE d.id = %s
            GROUP BY d.id;
        """

        cur.execute(query, (dish_id,))
        result = cur.fetchone()

        if result:
            kcal, carbs, sugar, fiber, protein, saturatedFat, monounSaturatedFat, polyunSaturatedFat, \
            transFat, cholesterol, vitaminA, vitaminC, vitaminD, vitaminB6, vitaminB12, \
            vitaminE, vitaminK, choline, canxi, fe, magie, photpho, kali, natri, zn, caffeine, alcohol = result

            if kcal >= 400:
                hashtags.append(15)
            elif kcal <= 100:
                hashtags.append(16)
                        
            if carbs >= 14:
                hashtags.append(19)
            elif carbs <= 55:
                hashtags.append(20)
                         
            if sugar >= 10:
                hashtags.append(21)
            elif sugar <= 2.5:
                hashtags.append(22)
            
            if fiber >= 5.5:
                hashtags.append(23)
            elif fiber <= 1.5:
                hashtags.append(24)  

            if protein >= 10:
                hashtags.append(17)
            elif protein <= 2.5:
                hashtags.append(18) 

            if saturatedFat >= 5:
                hashtags.append(25)
            elif saturatedFat <= 1:
                hashtags.append(26)   

            if monounSaturatedFat >= 10:
                hashtags.append(27)
            elif monounSaturatedFat <= 4:
                hashtags.append(28)      

            if polyunSaturatedFat >= 10:
                hashtags.append(29)
            elif polyunSaturatedFat <= 3:
                hashtags.append(30)  

            if transFat >= 1:
                hashtags.append(31)
            elif transFat <= 0.5:
                hashtags.append(32)      

            if cholesterol >= 10:
                hashtags.append(33)
            elif cholesterol <= 2.5:
                hashtags.append(34) 

            if vitaminA >= 60:
                hashtags.append(35)
            elif vitaminA <= 15:
                hashtags.append(36) 

            if vitaminC >= 18:
                hashtags.append(39)
            elif vitaminC <= 4.5:
                hashtags.append(40)         

            if vitaminD >= 4:
                hashtags.append(37)
            elif vitaminD <= 1:
                hashtags.append(38)  

            if vitaminB6 >= 0.34:
                hashtags.append(41)
            elif vitaminB6 <= 0.085:
                hashtags.append(42)

            if vitaminB12 >= 0.5:
                hashtags.append(43)
            elif vitaminB12 <= 0.12:
                hashtags.append(44)   

            if vitaminE >= 3:
                hashtags.append(45)
            elif vitaminE <= 0.75:
                hashtags.append(46)

            if vitaminK >= 24:
                hashtags.append(47)
            elif vitaminK <= 6:
                hashtags.append(48)

            if choline >= 110:
                hashtags.append(49)
            elif choline <= 27:
                hashtags.append(50)

            if canxi >= 260:
                hashtags.append(51)
            elif canxi <= 65:
                hashtags.append(52)

            if fe >= 3.6:
                hashtags.append(53)
            elif fe <= 0.9:
                hashtags.append(54)

            if magie >= 84:
                hashtags.append(55)
            elif magie <= 21:
                hashtags.append(56)

            if photpho >= 250:
                hashtags.append(57)
            elif photpho <= 62:
                hashtags.append(58)
            
            if kali >= 940:
                hashtags.append(59)
            elif kali <= 235:
                hashtags.append(60)

            if natri >= 460:
                hashtags.append(61)
            elif natri <= 115:
                hashtags.append(62)

            if zn >= 2.2:
                hashtags.append(63)
            elif zn <= 0.55:
                hashtags.append(64)
            
            if caffeine >= 100:
                hashtags.append(65)
            elif caffeine <= 20:
                hashtags.append(66)

            if alcohol >= 2:
                hashtags.append(67)
            elif alcohol <= 0.1:
                hashtags.append(68)
            
        return hashtags

    finally:
        if conn:
            conn.close()

def calculate_bmr(user: UserInfoRequest) -> float:
    if user.gender == "male":
        return 88.362 + 13.397 * user.weight + 4.799 * user.height - 5.677 * user.age
    else:
        return 447.593 + 9.247 * user.weight + 3.098 * user.height - 4.330 * user.age
    
def get_activity_factor(activity_level_id: int) -> float:
    activity_mapping = {
        1: 1.2,
        2: 1.375,
        3: 1.55,
        4: 1.725,
        5: 1.9
    }
    return activity_mapping.get(activity_level_id, 1.2)

def calculate_days_from_target(dateTarget: date) -> int:
    today = date.today()
    if isinstance(dateTarget, datetime):
        dateTarget = dateTarget.date()
    return (dateTarget - today).days

def get_extra_water(activity_level_id: int) -> float:
    extra_water_mapping = {
        1: 0,
        2: 400,
        3: 650,
        4: 900,
        5: 1250
    }
    return extra_water_mapping.get(activity_level_id, 0.0)

def rda_vitamin_a(gender: str, age: int) -> int:    
    if 4 <= age <= 8:
        return 1300
    elif 9 <= age <= 13:
        return 2000
    elif age >13:
        if gender == "male":
            return 3000
        elif gender == "female":
            return 2300
        else:
            return 0
    else:
        return 0

def rda_vitamin_d(age: int) -> float:
    if age <= 70:
        return 15.0
    elif age > 70:
        return 20.0
    else:
        return 0.0

def rda_vitamin_c(gender: str, age: int) -> int:
    if 4 <= age <= 8:
        base = 25
    elif 9 <= age <= 13:
        base = 45
    elif age >= 14:
        if gender.lower() == "male":
            base = 90
        elif gender.lower() == "female":
            base = 75
        else:
            base = 0
    else:
        base = 0
    return base

def rda_vitamin_b6(gender: str, age: int) -> float:
    if 4 <= age <= 8:
        return 0.6
    elif 9 <= age <= 13:
        return 1.0
    elif 14 <= age <= 18:
        if gender == "male":
            return 1.3
        elif gender == "female":
            return 1.2
        else:
            return 0
    elif 19 <= age <= 50:
        return 1.3
    elif age > 50:
        if gender == "male":
            return 1.7
        elif gender == "female":
            return 1.5
        else:
            return 0
    else:
        return 0

def rda_vitamin_b12(age: int) -> float:
    if 4 <= age <= 8:
        return 1.2
    elif 9 <= age <= 13:
        return 1.8
    elif age >= 14:
        return 2.4
    else:
        return 0.0

def rda_vitamin_e(age: int) -> int:
    if 4 <= age <= 8:
        return 7
    elif 9 <= age <= 13:
        return 11
    elif age >= 14:
        return 15
    else:
        return 0

def rda_vitamin_k(gender: str, age: int) -> int:
    if 4 <= age <= 8:
        return 55
    elif 9 <= age <= 13:
        return 60
    elif 14 <= age <= 18:
        return 75
    elif age > 18:
        if gender == "male":
            return 120
        elif gender == "female":
            return 90
        else:
            return 0
    else:
            return 0

def rda_choline(gender: str, age: int) -> int:
    if 4 <= age <= 8:
        return 250
    elif 9 <= age <= 13:
        return 375
    elif age > 13:
        if gender.lower() == "male":
            return 550
        elif gender.lower() == "female":
            return 425
        else:
            return 0
    else:
        return 0

def rda_canxi(gender: str, age: int) -> int:
    if 4 <= age <= 8:
        return 1000
    elif 9 <= age <= 18:
        return 1300
    elif 19 <= age <= 50:
        return 1000
    elif 51 <= age <= 70:
        if gender == "male":
            return 1000
        elif gender == "female":
            return 1200
        else:
            return 0
    elif age > 70:  
        return 1200
    else:  
        return 0

def rda_fe(gender: str, age: int) -> int:
    if 4 <= age <= 8:
        return 10
    elif 9 <= age <= 13:
        return 8
    elif 14 <= age <= 18:
        if gender == "male":
            return 11
        elif gender == "female":
            return 15
        else:
            return 0
    elif 19 <= age <= 50:
        if gender == "male":
            return 8
        elif gender == "female":
            return 18
        else:
            return 0
    elif 51 <= age <= 70:
        if gender == "male":
            return 8
        elif gender == "female":
            return 8
        else:
            return 0
    elif age > 70:
        return 8
    else:
        return 0

def rda_mg(gender: str, age: int) -> int:
    if 4 <= age <= 8:
        return 130
    elif 9 <= age <= 13:
        return 240
    elif 14 <= age <= 18:
        if gender == "male":
            return 410
        elif gender == "female":
            return 360
        else:
            return 0
    elif 19 <= age <= 30:
        if gender == "male":
            return 400
        elif gender == "female":
            return 310
        else:
            return 0
    elif 31 <= age <= 50:
        if gender == "male":
            return 420
        elif gender == "female":
            return 320
        else:
            return 0
    elif age >= 51:
        if gender == "male":
            return 420
        elif gender == "female":
            return 320
        else:
            return 0
    else:
        return 0

def rda_photpho(age: int) -> int:
    if 4 <= age <= 8:
        return 500
    elif 9 <= age <= 13:
        return 1250
    elif 14 <= age <= 18:
        return 1250
    elif 19 <= age:
        return 700
    else:
        return 0

def rda_kali(age: int) -> int:
    if 4 <= age <= 8:
        return 3800
    elif 9 <= age <= 13:
        return 3800
    elif 14 <= age <= 18:
        return 4500
    elif 19 <= age:
        return 4700
    else:
        return 0

def rda_natri(age: int) -> int:
    if 4 <= age <= 8:
        return 1900
    elif 9 <= age <= 13:
        return 2200
    elif 14 <= age:
        return 2300
    else:
        return 0

def rda_zn(gender: str, age: int) -> int:
    if 4 <= age <= 8:
        return 5
    elif 9 <= age <= 13:
        return 8
    elif 14 <= age <= 18:
        if gender == "male":
            return 11
        elif gender == "female":
            return 9
        else:
            return 0
    elif 19 <= age:
        if gender == "male":
            return 11
        elif gender == "female":
            return 8
        else:
            return 0
    else:
        return 0

def alcohol_limit(gender: str, age: int) -> int:
    if age < 18:
        return 1
    elif age >= 65:
        return 8
    else:
        if gender == "male":
            return 20
        elif gender == "female":
            return 10
        else:
            return 0

def get_hashtags_from_limit_food(limit_food_ids: list[int]) -> list[int]:
    banHashtags = []
    if 2 in limit_food_ids:
        banHashtags += [2]
    if 3 in limit_food_ids:
        banHashtags += [1, 2, 3]
    if 4 in limit_food_ids:
        banHashtags += [1]
    if 5 in limit_food_ids:
        banHashtags += [4]
    if 6 in limit_food_ids:
        banHashtags += [5]
    if 7 in limit_food_ids:
        banHashtags += [6]
    if 8 in limit_food_ids:
        banHashtags += [7]
    return banHashtags

def adjust_value(value: float, encourage: list[int], limit: list[int], max_rule: int, min_rule: int) -> float:
    if max_rule in encourage:
        value += value * (1/3)
    if min_rule in encourage:
        value -= value * (2/3)
    if max_rule in limit:
        value -= value * (1/3)
    return value

def build_required_index_data(user: UserInfoRequest) -> dict:
    bmr = calculate_bmr(user)
    tdee = get_activity_factor(user.ActivityLevelid)*bmr
    targetCalories=tdee+(user.weightTarget-user.weight)*7700/max(calculate_days_from_target(user.dateTarget),1)
    water=user.weight*30+get_extra_water(user.ActivityLevelid)

    banHashtags = []
    encourageHashtags = []
    limitHashtags = []
    
    banHashtags += get_hashtags_from_limit_food(user.LimitFoodid)

    match user.Dietid:
        case 1:
            protein=23
            totalFat=27
            carbohydrate=50
        case 2:
            protein=20
            totalFat=30
            carbohydrate=50
            encourageHashtags+= [11,12,5,4]
            limitHashtags+= [7,8,24,28,30]
        case 3:
            protein=30
            totalFat=30
            carbohydrate=40
            limitHashtags+= [18,19,21,28,30]
        case 4:
            protein=20
            totalFat=70
            carbohydrate=10
            encourageHashtags+= [20,22,27,29]
            limitHashtags+= [18]
        case 5:
            protein=35
            totalFat=20
            carbohydrate=45
            limitHashtags+= [18,19,21,25,31,27,29]
        case 6:
            protein=35
            totalFat=45
            carbohydrate=20
            banHashtags+= [8]
            encourageHashtags+= [2,3,11,12]
            limitHashtags+= [1,6,9,13]

    if user.Dietid==1 and 1 not in user.HealthStatusid:
        if 17 in encourageHashtags:
            protein+=8
            totalFat-=4
            carbohydrate-=4
        if 18 in encourageHashtags:
            protein-=8
            totalFat+=4
            carbohydrate+=4
        if 17 in limitHashtags:
            protein-=5
            totalFat+=2
            carbohydrate+=3

        high_carb = {19, 21, 23}
        low_carb  = {20, 22, 24}
        encourage_high = any(tag in encourageHashtags for tag in high_carb)
        encourage_low  = any(tag in encourageHashtags for tag in low_carb)
        limit_high = any(tag in limitHashtags for tag in high_carb)
        limit_low  = any(tag in limitHashtags for tag in low_carb)
        if encourage_high and not encourage_low and not limit_high:
            carbohydrate += 8
            protein -= 4
            totalFat -= 4
        elif encourage_low and not encourage_high and not limit_low:
            carbohydrate -= 8
            protein += 4
            totalFat += 4

        high_fat = {25, 27, 29, 31}
        low_fat  = {26, 28, 30, 32}

        encourage_high_fat = any(tag in encourageHashtags for tag in high_fat)
        encourage_low_fat  = any(tag in encourageHashtags for tag in low_fat)

        limit_high_fat = any(tag in limitHashtags for tag in high_fat)
        limit_low_fat  = any(tag in limitHashtags for tag in low_fat)

        if encourage_high_fat and not encourage_low_fat and not limit_high_fat:
            totalFat += 8
            protein -= 4
            carbohydrate -= 4

        elif encourage_low_fat and not encourage_high_fat and not limit_low_fat:
            totalFat -= 8
            protein += 4
            carbohydrate += 4

    carbohydrate_g = (targetCalories * carbohydrate / 100) / 4
    protein_g      = (targetCalories * protein / 100) / 4
    fat_g          = (targetCalories * totalFat / 100) / 9

    carbs=75
    sugar=10
    fiber=15
    addedCarb=0
    if 19 in encourageHashtags:
        carbs+=5
        addedCarb-=5
    if 20 in encourageHashtags:
        carbs-=5
        addedCarb+=5
    if 19 in limitHashtags:
        carbs-=3
        addedCarb+=3
    if 21 in encourageHashtags:
        sugar+=5
        addedCarb-=5
    if 22 in encourageHashtags:
        sugar-=5
        addedCarb+=5
    if 21 in limitHashtags:
        sugar-=3
        addedCarb+=3
    if 23 in encourageHashtags:
        fiber+=5
        addedCarb-=5
    if 24 in encourageHashtags:
        fiber-=5
        addedCarb+=5
    if 23 in limitHashtags:
        fiber-=3
        addedCarb+=3
    carbs += addedCarb
    addedCarb=0
    carbs_g = carbohydrate_g * carbs / 100
    sugar_g = carbohydrate_g * sugar / 100
    fiber_g = carbohydrate_g * fiber / 100

    saturatedFat=34
    monounsaturatedFat=45
    polyunsaturatedFat=20
    transFat=1
    addedFat=0
    if 31 in encourageHashtags:
        saturatedFat=33
        transFat=2
    if 32 in encourageHashtags or 31 in limitHashtags:
        saturatedFat=35
        transFat=0
    if 25 in encourageHashtags:
        saturatedFat+=5
        addedFat-=5
    if 26 in encourageHashtags:
        saturatedFat-=5
        addedFat+=5
    if 25 in limitHashtags:
        saturatedFat-=3
        addedFat+=3  
    if 27 in encourageHashtags:
        monounsaturatedFat+=5
        addedFat-=5
    if 28 in encourageHashtags:
        monounsaturatedFat-=5
        addedFat+=5
    if 27 in limitHashtags:
        monounsaturatedFat-=3
        addedFat+=3  
    if 29 in encourageHashtags:
        polyunsaturatedFat+=5
        addedFat-=5
    if 30 in encourageHashtags:
        polyunsaturatedFat-=5
        addedFat+=5
    if 29 in limitHashtags:
        polyunsaturatedFat-=3
        addedFat+=3 
    share = addedFat / 3
    saturatedFat += share
    monounsaturatedFat += share
    polyunsaturatedFat += share
    addedFat=0 
    saturatedFat_g = fat_g * saturatedFat / 100
    monounsaturatedFat_g = fat_g * monounsaturatedFat / 100
    polyunsaturatedFat_g = fat_g * polyunsaturatedFat / 100
    transFat_g = fat_g * transFat / 100

    cholesterol=adjust_value(300,encourageHashtags,limitHashtags,33,34)
    vitA=adjust_value(rda_vitamin_a(user.gender,user.age),encourageHashtags,limitHashtags,35,36)
    vitD=adjust_value(rda_vitamin_d(user.age),encourageHashtags,limitHashtags,37,38)
    vitC=adjust_value(rda_vitamin_c(user.gender,user.age),encourageHashtags,limitHashtags,39,40)
    vitB6=adjust_value(rda_vitamin_b6(user.gender,user.age),encourageHashtags,limitHashtags,41,42)
    vitB12=adjust_value(rda_vitamin_b12(user.age),encourageHashtags,limitHashtags,43,44)
    vitE=adjust_value(rda_vitamin_e(user.age),encourageHashtags,limitHashtags,45,46)
    vitK=adjust_value(rda_vitamin_k(user.gender,user.age),encourageHashtags,limitHashtags,47,48)
    choline=adjust_value(rda_choline(user.gender,user.age),encourageHashtags,limitHashtags,49,50)
    canxi=adjust_value(rda_canxi(user.gender,user.age),encourageHashtags,limitHashtags,51,52)
    fe=adjust_value(rda_fe(user.gender,user.age),encourageHashtags,limitHashtags,53,54)
    magie=adjust_value(rda_mg(user.gender,user.age),encourageHashtags,limitHashtags,55,56)
    photpho=adjust_value(rda_photpho(user.age),encourageHashtags,limitHashtags,57,58)
    kali=adjust_value(rda_kali(user.age),encourageHashtags,limitHashtags,59,60)
    natri=adjust_value(rda_natri(user.age),encourageHashtags,limitHashtags,61,62)
    zn=adjust_value(rda_zn(user.gender,user.age),encourageHashtags,limitHashtags,63,64)
    caffeine=adjust_value(3*user.weight,encourageHashtags,limitHashtags,65,66)
    alcohol=adjust_value(alcohol_limit(user.gender,user.age),encourageHashtags,limitHashtags,67,68)

    return {
    "requiredIndex": {
        "bmr": bmr,
        "tdee": tdee,
        "targetCalories": targetCalories,
        "water": water,
        "protein": protein_g,
        "totalFat": fat_g,
        "saturatedFat": saturatedFat_g,
        "monounSaturatedFat": monounsaturatedFat_g,
        "polyunSaturatedFat": polyunsaturatedFat_g,
        "transFat": transFat_g,
        "carbohydrate": carbohydrate_g,
        "carbs": carbs_g,
        "sugar": sugar_g,
        "fiber": fiber_g,
        "cholesterol": cholesterol,
        "vitaminA": vitA,
        "vitaminD": vitD,
        "vitaminC": vitC,
        "vitaminB6": vitB6,
        "vitaminB12": vitB12,
        "vitaminE": vitE,
        "vitaminK": vitK,
        "choline": choline,
        "canxi": canxi,
        "fe": fe,
        "magie": magie,
        "photpho": photpho,
        "kali": kali,
        "natri": natri,
        "zn": zn,
        "caffeine": caffeine,
        "alcohol": alcohol,
    },
    "banHashtags": banHashtags,
    "encourageHashtags": encourageHashtags,
    "limitHashtags": limitHashtags,
}
