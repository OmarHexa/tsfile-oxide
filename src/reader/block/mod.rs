// Block readers produce columnar TsBlock batches from on-disk data.
// 5b-i adds SingleDeviceTsBlockReader (time-aligned k-way merge for one
// non-aligned device's N measurement scanners). DeviceOrderedTsBlockReader
// (multi-device walk for the table model) is deferred to 5b-ii.

pub mod single_device;

// pub use single_device::SingleDeviceTsBlockReader; // re-enabled in Task 2
