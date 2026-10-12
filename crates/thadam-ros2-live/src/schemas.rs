//! Assembling `ros2msg` schema text from `msg_dirs`.
//!
//! A live stream has no recording to read schemas from, so the chain's
//! `decode: ros2` is fed the same shape MCAP carries: the concatenated
//! multi-definition format — the main `.msg` definition, then one
//! separator + `MSG: pkg/Type` section per transitively referenced type
//! (what [`Registry::register_ros2msg`] parses). Definitions are found in
//! the ROS package layout: `<dir>/<pkg>/msg/<Type>.msg`, trying each
//! `msg_dirs` entry in order.

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

use thadam_ros2_msg::Registry;
use thadam_ros2_msg::msg::{ScalarType, TypeKind, TypeName};

use crate::error::Ros2LiveError;

/// The MCAP/rosbag2 section separator (any run of 10+ `=` parses; the
/// recorders write 80).
const SEPARATOR: &str =
    "================================================================================";

/// One type's assembled schema, ready for the schema store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssembledSchema {
    /// The canonical schema name, `pkg/msg/Type` (the spelling MCAP records
    /// use for `ros2msg` schemas).
    pub name: String,
    /// The concatenated MCAP-style definition text.
    pub text: String,
}

/// Assembles the `ros2msg` document for `ros_type` (spelled `pkg/msg/Type`
/// or `pkg/Type`) out of the `.msg` files under `msg_dirs`, following nested
/// type references depth-first. The assembled text is registered and
/// compiled before it is returned, so an unreadable or unresolvable
/// definition fails here rather than surprising the pipeline mid-run.
pub fn assemble(msg_dirs: &[PathBuf], ros_type: &str) -> Result<AssembledSchema, Ros2LiveError> {
    let main = TypeName::parse(ros_type).map_err(|e| Ros2LiveError::Schema {
        type_name: ros_type.to_string(),
        detail: e.to_string(),
    })?;
    let mut registry = Registry::new();
    let mut seen: HashSet<TypeName> = HashSet::from([main.clone()]);
    let mut nested: Vec<(TypeName, String)> = Vec::new();
    let main_text = visit(msg_dirs, &main, &mut seen, &mut registry, &mut nested)?;

    let name = format!("{}/msg/{}", main.package(), main.name());
    let mut text = main_text.trim_end().to_string();
    text.push('\n');
    for (dep, dep_text) in &nested {
        text.push_str(SEPARATOR);
        text.push('\n');
        text.push_str(&format!("MSG: {dep}\n"));
        text.push_str(dep_text.trim_end());
        text.push('\n');
    }

    // Round-trip the document exactly as the chain decoder will (the store
    // text is registered with `register_ros2msg` downstream), then compile,
    // so a schema that cannot decode fails at source build.
    let mut check = Registry::new();
    let main_name = check
        .register_ros2msg(&name, &text)
        .map_err(|e| Ros2LiveError::Schema {
            type_name: name.clone(),
            detail: e.to_string(),
        })?;
    check
        .compile(&main_name.to_string())
        .map_err(|e| Ros2LiveError::Schema {
            type_name: name.clone(),
            detail: e.to_string(),
        })?;
    Ok(AssembledSchema { name, text })
}

/// Registers `type_name`'s definition and recurses into every named type it
/// references, appending each newly seen nested definition (post-order) to
/// `nested`. Returns the definition text of `type_name` itself.
fn visit(
    msg_dirs: &[PathBuf],
    type_name: &TypeName,
    seen: &mut HashSet<TypeName>,
    registry: &mut Registry,
    nested: &mut Vec<(TypeName, String)>,
) -> Result<String, Ros2LiveError> {
    let text = load(msg_dirs, type_name)?;
    registry
        .register(type_name.package(), type_name.name(), &text)
        .map_err(|e| Ros2LiveError::Schema {
            type_name: type_name.to_string(),
            detail: e.to_string(),
        })?;
    let spec = registry
        .spec(&type_name.to_string())
        .ok_or_else(|| Ros2LiveError::Schema {
            type_name: type_name.to_string(),
            detail: "definition did not register".into(),
        })?;
    // Collect references first: `spec` borrows the registry.
    let mut refs = Vec::new();
    for field in spec.fields() {
        let named = match &field.ty {
            TypeKind::Scalar(ScalarType::Named(named)) => Some(named),
            TypeKind::Array {
                elem: ScalarType::Named(named),
                ..
            } => Some(named),
            _ => None,
        };
        if let Some(named) = named {
            let package = named
                .package
                .clone()
                .unwrap_or_else(|| type_name.package().to_string());
            refs.push((package, named.name.clone()));
        }
    }
    for (package, name) in refs {
        let dep = TypeName::new(&package, &name).map_err(|e| Ros2LiveError::Schema {
            type_name: format!("{package}/{name}"),
            detail: e.to_string(),
        })?;
        if seen.insert(dep.clone()) {
            // A definition cycle surfaces as the compiler's
            // `schema.invalid_shape` on the round-trip above, not a hang:
            // `seen` breaks the recursion.
            let dep_text = visit(msg_dirs, &dep, seen, registry, nested)?;
            nested.push((dep, dep_text));
        }
    }
    Ok(text)
}

/// Reads `<dir>/<pkg>/msg/<Type>.msg`, trying each `msg_dirs` entry.
fn load(msg_dirs: &[PathBuf], type_name: &TypeName) -> Result<String, Ros2LiveError> {
    let relative = format!("{}/msg/{}.msg", type_name.package(), type_name.name());
    for dir in msg_dirs {
        match fs::read_to_string(dir.join(&relative)) {
            Ok(text) => return Ok(text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(Ros2LiveError::Schema {
                    type_name: type_name.to_string(),
                    detail: format!("{}: {e}", dir.join(&relative).display()),
                });
            }
        }
    }
    Err(Ros2LiveError::MsgNotFound {
        relative,
        tried: msg_dirs.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Ros2LiveError;

    const TIME: &str = "int32 sec\nuint32 nanosec\n";
    const HEADER: &str = "builtin_interfaces/Time stamp\nstring frame_id\n";
    const VECTOR3: &str = "float64 x\nfloat64 y\nfloat64 z\n";
    const QUATERNION: &str = "float64 x 0\nfloat64 y 0\nfloat64 z 0\nfloat64 w 1\n";
    const IMU: &str = "std_msgs/Header header\n\
                        geometry_msgs/Quaternion orientation\n\
                        float64[9] orientation_covariance\n\
                        geometry_msgs/Vector3 angular_velocity\n\
                        float64[9] angular_velocity_covariance\n\
                        geometry_msgs/Vector3 linear_acceleration\n\
                        float64[9] linear_acceleration_covariance\n";

    fn msg_tree(dir: &std::path::Path) {
        let write = |rel: &str, text: &str| {
            let path = dir.join(rel);
            fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
            fs::write(path, text).expect("write");
        };
        write("builtin_interfaces/msg/Time.msg", TIME);
        write("std_msgs/msg/Header.msg", HEADER);
        write("geometry_msgs/msg/Vector3.msg", VECTOR3);
        write("geometry_msgs/msg/Quaternion.msg", QUATERNION);
        write("sensor_msgs/msg/Imu.msg", IMU);
    }

    #[test]
    fn assembly_concatenates_definitions_depth_first_and_compiles() {
        let dir = tempfile::tempdir().expect("temp dir");
        msg_tree(dir.path());
        let schema =
            assemble(&[dir.path().to_path_buf()], "sensor_msgs/msg/Imu").expect("assemble");
        assert_eq!(schema.name, "sensor_msgs/msg/Imu");

        // Post-order DFS: Time (Header's dependency), Header, Quaternion,
        // Vector3 follow the main Imu definition.
        let sep_sections: Vec<&str> = schema.text.split(SEPARATOR).collect();
        assert_eq!(sep_sections.len(), 5, "{}\n---", schema.text);
        assert!(sep_sections[0].starts_with("std_msgs/Header header\n"));
        assert!(sep_sections[1].starts_with("\nMSG: builtin_interfaces/Time\n"));
        assert!(sep_sections[2].starts_with("\nMSG: std_msgs/Header\n"));
        assert!(sep_sections[3].starts_with("\nMSG: geometry_msgs/Quaternion\n"));
        assert!(sep_sections[4].starts_with("\nMSG: geometry_msgs/Vector3\n"));

        // The document parses and compiles through the decoder's own path.
        let mut registry = Registry::new();
        registry
            .register_ros2msg("sensor_msgs/msg/Imu", &schema.text)
            .expect("round-trip registers");
        let compiled = registry
            .compile("sensor_msgs/Imu")
            .expect("round-trip compiles");
        assert!(compiled.shape().index_of("header.stamp").is_some());
        assert!(compiled.shape().index_of("angular_velocity.x").is_some());
    }

    #[test]
    fn shared_nested_types_appear_once_and_pkg_slash_type_spelling_works() {
        let dir = tempfile::tempdir().expect("temp dir");
        msg_tree(dir.path());
        // The `pkg/Type` spelling works, and shared references (Time via
        // Header; Quaternion/Vector3 each referenced once) are not repeated.
        let schema = assemble(&[dir.path().to_path_buf()], "sensor_msgs/Imu").expect("assemble");
        assert_eq!(schema.text.matches("MSG: ").count(), 4, "{}", schema.text);
        assert_eq!(
            schema.text.matches("MSG: builtin_interfaces/Time").count(),
            1
        );
    }

    #[test]
    fn a_missing_definition_names_what_was_searched() {
        let dir = tempfile::tempdir().expect("temp dir");
        msg_tree(dir.path());
        fs::remove_file(dir.path().join("geometry_msgs/msg/Quaternion.msg")).expect("rm");
        let error = assemble(&[dir.path().to_path_buf()], "sensor_msgs/msg/Imu")
            .expect_err("missing nested definition fails");
        let Ros2LiveError::MsgNotFound { relative, tried } = error else {
            panic!("wrong variant: {error}");
        };
        assert_eq!(relative, "geometry_msgs/msg/Quaternion.msg");
        assert_eq!(tried, vec![dir.path().to_path_buf()]);
    }

    #[test]
    fn fallback_dirs_are_searched_in_order() {
        let empty = tempfile::tempdir().expect("temp dir");
        let full = tempfile::tempdir().expect("temp dir");
        msg_tree(full.path());
        let schema = assemble(
            &[empty.path().to_path_buf(), full.path().to_path_buf()],
            "sensor_msgs/msg/Imu",
        )
        .expect("falls back to the second dir");
        assert!(schema.text.contains("MSG: std_msgs/Header"));
    }

    #[test]
    fn an_unparseable_definition_fails_as_schema_error() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("a_msgs/msg/Broken.msg");
        fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        // A type token with no field name violates the .msg grammar.
        fs::write(&path, "int32\n").expect("write");
        let error = assemble(&[dir.path().to_path_buf()], "a_msgs/msg/Broken").expect_err("fails");
        assert!(matches!(error, Ros2LiveError::Schema { .. }), "{error}");
    }
}
