// RESP (Redis Serialization Protocol) parser and formatter
const std = @import("std");

pub const ParseError = error{ InvalidRespType, IncompleteData, InvalidBulkStringLength, IncompleteBulkString, InvalidArrayLength, IncompleteArray } || std.fmt.ParseIntError || std.mem.Allocator.Error;

pub const RespValue = union(enum) {
    simple_string: []const u8,
    error_msg: []const u8,
    integer: i64,
    bulk_string: ?[]const u8,
    array: []RespValue,
};

pub const RespParser = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) RespParser {
        return RespParser{ .allocator = allocator };
    }

    pub fn parse(self: *RespParser, data: []const u8) ParseError!?RespValue {
        if (data.len == 0) return null;

        var pos: usize = 0;
        return try self.parseValue(data, &pos);
    }

    fn parseValue(self: *RespParser, data: []const u8, pos: *usize) ParseError!?RespValue {
        if (pos.* >= data.len) return null;

        const type_byte = data[pos.*];
        pos.* += 1;

        return switch (type_byte) {
            '+' => try self.parseSimpleString(data, pos),
            '-' => try self.parseError(data, pos),
            ':' => try self.parseInteger(data, pos),
            '$' => try self.parseBulkString(data, pos),
            '*' => try self.parseArray(data, pos),
            else => error.InvalidRespType,
        };
    }

    fn parseSimpleString(self: *RespParser, data: []const u8, pos: *usize) ParseError!RespValue {
        _ = self;

        const line = try readLine(data, pos);
        return RespValue{ .simple_string = line };
    }

    fn parseError(self: *RespParser, data: []const u8, pos: *usize) ParseError!RespValue {
        _ = self;

        const line = try readLine(data, pos);
        return RespValue{ .error_msg = line };
    }

    fn parseInteger(self: *RespParser, data: []const u8, pos: *usize) ParseError!RespValue {
        _ = self;

        const line = try readLine(data, pos);
        const value = try std.fmt.parseInt(i64, line, 10);
        return RespValue{ .integer = value };
    }

    fn parseBulkString(self: *RespParser, data: []const u8, pos: *usize) ParseError!RespValue {
        _ = self;

        const line = try readLine(data, pos);
        const length = try std.fmt.parseInt(i64, line, 10);

        if (length == -1) {
            return RespValue{ .bulk_string = null };
        }

        if (length < 0) {
            return error.InvalidBulkStringLength;
        }

        const len = @as(usize, @intCast(length));
        if (pos.* + len > data.len) return error.IncompleteBulkString;

        const str = data[pos.* .. pos.* + len];
        pos.* += len;

        if (pos.* + 2 > data.len) return error.IncompleteData;
        pos.* += 2;

        return RespValue{ .bulk_string = str };
    }

    fn parseArray(self: *RespParser, data: []const u8, pos: *usize) ParseError!RespValue {
        const line = try readLine(data, pos);

        std.debug.print("parseArray: line = '{s}' (length: {d})\n", .{ line, line.len });
        std.debug.print("parseArray: line bytes = ", .{});

        for (line) |byte| {
            std.debug.print("{x:0>2}", .{byte});
        }

        std.debug.print("\n", .{});

        const count = try std.fmt.parseInt(i64, line, 10);

        std.debug.print("parseArray: count = {d}\n", .{count});

        if (count < 0) return error.InvalidArrayLength;

        const len = @as(usize, @intCast(count));
        var array = try self.allocator.alloc(RespValue, len);

        for (0..len) |i| {
            if (try self.parseValue(data, pos)) |value| {
                array[i] = value;
            } else return error.IncompleteArray;
        }

        return RespValue{ .array = array };
    }

    fn readLine(data: []const u8, pos: *usize) ![]const u8 {
        const start = pos.*;

        std.debug.print("readLine: starting at pos {d}, looking in data[{d}..{d}]\n", .{ start, start, data.len });

        while (pos.* < data.len) {
            const valueAtPos = data[pos.*];
            const valueAtPosPlusOne = data[pos.* + 1];

            if (valueAtPos == '\r' and pos.* + 1 < data.len and valueAtPosPlusOne == '\n') {
                const line = data[start..pos.*];

                std.debug.print("readLine: found line '{s}' at pos {d}\n", .{ line, start });

                pos.* += 2;
                return line;
            }

            pos.* += 1;
        }

        std.debug.print("readLine: reached end without finding \\r\\n\n", .{});
        return error.IncompleteData;
    }

    pub fn freeValue(self: *RespParser, value: RespValue) void {
        switch (value) {
            .array => |arr| {
                for (arr) |item| {
                    self.freeValue(item);
                }

                self.allocator.free(arr);
            },
            else => {},
        }
    }
};

pub fn formatBulkString(allocator: std.mem.Allocator, str: ?[]const u8) ![]u8 {
    if (str) |s| {
        return try std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ s.len, s });
    } else {
        return try allocator.dupe(u8, "$-1\r\n");
    }
}

pub fn formatError(allocator: std.mem.Allocator, err: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, "-{s}\r\n", .{err});
}

pub fn formatSimpleString(allocator: std.mem.Allocator, str: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, "+{s}\r\n", .{str});
}

pub fn formatInteger(allocator: std.mem.Allocator, value: i64) ![]u8 {
    return try std.fmt.allocPrint(allocator, ":{d}\r\n", .{value});
}

pub fn formatArray(allocator: std.mem.Allocator, items: [][]const u8) ![]u8 {
    std.debug.print("formatArray called with {d} items\n", .{items.len});

    var result = std.ArrayListUnmanaged(u8){};
    errdefer result.deinit(allocator);

    try result.writer(allocator).print("*{d}\r\n", .{items.len});

    for (items, 0..) |item, i| {
        std.debug.print("Item {d}: '{s}' (len={d})\n", .{ i, item, item.len });
        try result.writer(allocator).print("${d}\r\n{s}\r\n", .{ item.len, item });
    }

    std.debug.print("formatArray completed\n", .{});

    return try result.toOwnedSlice(allocator);
}
