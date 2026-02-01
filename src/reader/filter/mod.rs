// C++ filter system uses Filter* base class with 20+ virtual subclasses.
// Filters are composed at runtime into trees (AndFilter holds Filter* left/right).
// This is one of the few places we use dyn Trait instead of enum — the
// filter set is open (users should be able to add custom filters) and
// filters are composed dynamically via Box<dyn Filter>.
