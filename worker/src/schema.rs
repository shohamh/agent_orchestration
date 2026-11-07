//! The part of JSON Schema that agents' question forms use: enough to refuse an answer the form does not allow.
use serde_json::Value;

fn type_matches(value: &Value, kind: &str) -> bool {
    match kind {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => {
            value.as_i64().is_some()
                || value.as_u64().is_some()
                || value.as_f64().map(|f| f.fract() == 0.0).unwrap_or(false)
        }
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => true,
    }
}

pub fn validate(value: &Value, schema: &Value) -> Result<(), String> {
    let Some(rules) = schema.as_object() else {
        return Ok(());
    };
    match rules.get("type") {
        Some(Value::String(kind)) if !type_matches(value, kind) => {
            return Err(format!("{value} is not of type '{kind}'"))
        }
        Some(Value::Array(kinds))
            if !kinds
                .iter()
                .any(|k| k.as_str().map(|k| type_matches(value, k)).unwrap_or(false)) =>
        {
            return Err(format!("{value} has the wrong type"))
        }
        _ => {}
    }
    if let Some(allowed) = rules.get("const") {
        if value != allowed {
            return Err(format!("{allowed} was expected"));
        }
    }
    if let Some(Value::Array(allowed)) = rules.get("enum") {
        if !allowed.contains(value) {
            return Err(format!("{value} is not one of the choices"));
        }
    }
    if let Some(Value::Array(choices)) = rules.get("oneOf") {
        if choices
            .iter()
            .filter(|c| validate(value, c).is_ok())
            .count()
            != 1
        {
            return Err(format!("{value} is not one of the choices"));
        }
    }
    if let Some(Value::Array(choices)) = rules.get("anyOf") {
        if !choices.iter().any(|c| validate(value, c).is_ok()) {
            return Err(format!("{value} is not one of the choices"));
        }
    }
    if let Some(text) = value.as_str() {
        let length = text.chars().count() as u64;
        if rules
            .get("minLength")
            .and_then(|m| m.as_u64())
            .map(|m| length < m)
            .unwrap_or(false)
        {
            return Err("The answer is too short".into());
        }
        if rules
            .get("maxLength")
            .and_then(|m| m.as_u64())
            .map(|m| length > m)
            .unwrap_or(false)
        {
            return Err("The answer is too long".into());
        }
    }
    if let Some(number) = value.as_f64() {
        if rules
            .get("minimum")
            .and_then(|m| m.as_f64())
            .map(|m| number < m)
            .unwrap_or(false)
        {
            return Err(format!("{number} is below the minimum"));
        }
        if rules
            .get("maximum")
            .and_then(|m| m.as_f64())
            .map(|m| number > m)
            .unwrap_or(false)
        {
            return Err(format!("{number} is above the maximum"));
        }
    }
    if let Some(items) = value.as_array() {
        let count = items.len() as u64;
        if rules
            .get("minItems")
            .and_then(|m| m.as_u64())
            .map(|m| count < m)
            .unwrap_or(false)
        {
            return Err("Too few choices".into());
        }
        if rules
            .get("maxItems")
            .and_then(|m| m.as_u64())
            .map(|m| count > m)
            .unwrap_or(false)
        {
            return Err("Too many choices".into());
        }
        if let Some(each) = rules.get("items") {
            for item in items {
                validate(item, each)?;
            }
        }
    }
    if let Some(object) = value.as_object() {
        if let Some(Value::Array(required)) = rules.get("required") {
            for name in required.iter().filter_map(|n| n.as_str()) {
                if !object.contains_key(name) {
                    return Err(format!("'{name}' is a required property"));
                }
            }
        }
        let properties = rules.get("properties").and_then(|p| p.as_object());
        for (name, item) in object {
            match properties.and_then(|p| p.get(name)) {
                Some(rule) => validate(item, rule)?,
                None if rules.get("additionalProperties") == Some(&Value::Bool(false)) => {
                    return Err(format!("'{name}' was not expected"))
                }
                None => {}
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate;
    use serde_json::json;

    #[test]
    fn answers_are_held_to_the_form() {
        let form = json!({"type": "object", "required": ["style"], "properties": {
            "style": {"type": "string", "oneOf": [{"const": "brief"}, {"const": "detailed"}]},
            "checks": {"type": "array", "items": {"anyOf": [{"const": "tests"}]}},
            "count": {"type": "integer", "minimum": 1}}});
        assert!(validate(&json!({"style": "brief"}), &form).is_ok());
        assert!(validate(
            &json!({"style": "brief", "checks": ["tests"], "count": 2}),
            &form
        )
        .is_ok());
        assert!(validate(&json!({}), &form).is_err());
        assert!(validate(&json!({"style": "other"}), &form).is_err());
        assert!(validate(&json!({"style": "brief", "checks": ["nope"]}), &form).is_err());
        assert!(validate(&json!({"style": "brief", "count": 0}), &form).is_err());
        assert!(validate(&json!("text"), &form).is_err());
    }
}
