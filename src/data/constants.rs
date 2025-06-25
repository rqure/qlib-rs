
pub const INDIRECTION_DELIMITER: &str = "->";

pub mod et {
    pub const OBJECT: &str = "Object"; // All entity types inherit from this

    pub const ROOT : &str = "Root";
}

pub mod ft {
    pub const NAME : &str = "Name"; // Belongs to Object
    pub const PARENT: &str = "Parent"; // Belongs to Object
    pub const CHILDREN: &str = "Children"; // Belongs to Object
}