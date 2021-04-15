SELECT user_id,
			 user.first_name,
			 user.last_name,
			 user.age,
			 user.orders
	FROM nested_json
	WHERE user_id = 1