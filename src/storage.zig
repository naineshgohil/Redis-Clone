const std = @import("std");

pub const Storage = struct {
    map: std.StringHashMap([]const u8),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Storage {
        return Storage{
            .map = std.StringHashMap([]const u8).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Storage) void {
        var it = self.map.iterator();

        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }

        self.map.deinit();
    }

    pub fn set(self: *Storage, key: []const u8, value: []const u8) !void {
        // Check if key already exists
        if (self.map.get(key)) |old_value| {
            self.allocator.free(old_value);
            _ = self.map.remove(key);
        }

        // Duplicate the key and value to own the memory
        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);

        const value_copy = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(value_copy);

        try self.map.put(key_copy, value_copy);
    }

    pub fn get(self: *Storage, key: []const u8) ?[]const u8 {
        return self.map.get(key);
    }
};
