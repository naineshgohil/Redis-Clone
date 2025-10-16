const std = @import("std");
const server = @import("server.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    const port: u16 = 6379;

    var srv = try server.Server.init(allocator, port);
    defer srv.deinit();

    try srv.run();
}
