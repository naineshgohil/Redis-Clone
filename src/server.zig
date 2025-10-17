// TCP server with event loop and command handling

const std = @import("std");
const resp = @import("resp.zig");
const storage = @import("storage.zig");

pub const Server = struct {
    allocator: std.mem.Allocator,
    storage: storage.Storage,
    address: std.net.Address,

    pub fn init(allocator: std.mem.Allocator, port: u16) !Server {
        const address = try std.net.Address.parseIp("127.0.0.1", port);
        return Server{ .allocator = allocator, .storage = storage.Storage.init(allocator), .address = address };
    }

    pub fn deinit(self: *Server) void {
        self.storage.deinit();
    }

    pub fn run(self: *Server) !void {
        var server = try self.address.listen(.{ .reuse_address = true });
        defer server.deinit();

        std.debug.print("Redis-Zig server listening on 127.0.0.1:{d}\n", .{self.address.getPort()});

        while (true) {
            const conn = try server.accept();
            const port = conn.address.getPort();

            std.debug.print("Client connected from port: {d}\n", .{port});

            // Handle connection (for now, synchronously)
            self.handleConnection(conn) catch |err| {
                std.debug.print("Error handling connection: {any}\n", .{err});
            };
        }
    }

    fn handleConnection(self: *Server, conn: std.net.Server.Connection) !void {
        defer conn.stream.close();

        var buffer: [4096]u8 = undefined;
        var parser = resp.RespParser.init(self.allocator);

        while (true) {
            const bytes_read = conn.stream.read(&buffer) catch |err| {
                if (err == error.ConnectionResetByPeer) break;
                return err;
            };

            if (bytes_read == 0) break;

            const data = buffer[0..bytes_read];

            std.debug.print("Received {d} bytes: {s}\n", .{ bytes_read, data });

            const value = parser.parse(data) catch |err| {
                std.debug.print("Parse error: {any}\n", .{err});
                std.debug.print("Data was: {s}\n", .{data});
                const error_resp = try resp.formatError(self.allocator, "ERR Protocol error");
                defer self.allocator.free(error_resp);
                _ = try conn.stream.write(error_resp);
                continue;
            };

            if (value) |v| {
                defer parser.freeValue(v);
                const response = try self.handleCommand(v);
                defer self.allocator.free(response);

                _ = try conn.stream.write(response);
            }
        }

        std.debug.print("Clients disconnected\n", .{});
    }

    fn handleCommand(self: *Server, value: resp.RespValue) ![]u8 {
        switch (value) {
            .array => |arr| {
                if (arr.len == 0) {
                    return try resp.formatError(self.allocator, "ERR empty command");
                }

                const cmd = switch (arr[0]) {
                    .bulk_string => |s| s orelse {
                        return try resp.formatError(self.allocator, "ERR invalid command");
                    },
                    else => {
                        return try resp.formatError(self.allocator, "ERR invalid command format");
                    },
                };

                const cmd_upper = try self.allocator.alloc(u8, cmd.len);
                defer self.allocator.free(cmd_upper);

                _ = std.ascii.upperString(cmd_upper, cmd);

                if (std.mem.eql(u8, cmd_upper, "PING")) {
                    return try resp.formatSimpleString(self.allocator, "PONG");
                } else if (std.mem.eql(u8, cmd_upper, "SET")) {
                    return try self.handleSet(arr);
                } else if (std.mem.eql(u8, cmd_upper, "GET")) {
                    return try self.handleGet(arr);
                } else if (std.mem.eql(u8, cmd_upper, "DEL")) {
                    return try self.handleDel(arr);
                } else if (std.mem.eql(u8, cmd_upper, "EXISTS")) {
                    return try self.handleExists(arr);
                } else if (std.mem.eql(u8, cmd_upper, "EXPIRE")) {
                    return try self.handleExpire(arr);
                } else if (std.mem.eql(u8, cmd_upper, "TTL")) {
                    return try self.handleTTL(arr);
                } else if (std.mem.eql(u8, cmd_upper, "PERSIST")) {
                    return try self.handlePersist(arr);
                } else if (std.mem.eql(u8, cmd_upper, "KEYS")) {
                    return try self.handleKeys(arr);
                } else {
                    return try resp.formatError(self.allocator, "ERR unknown command");
                }
            },
            else => {
                return try resp.formatError(self.allocator, "ERR expected array");
            },
        }
    }

    fn handleSet(self: *Server, args: []resp.RespValue) ![]u8 {
        if (args.len != 3) {
            return try resp.formatError(self.allocator, "ERR wrong number of arguments for 'set' command");
        }

        const key = switch (args[1]) {
            .bulk_string => |s| s orelse {
                return try resp.formatError(self.allocator, "ERR invalid key");
            },
            else => {
                return try resp.formatError(self.allocator, "ERR invalid key type");
            },
        };

        const value = switch (args[2]) {
            .bulk_string => |s| s orelse {
                return try resp.formatError(self.allocator, "ERR invalid value");
            },
            else => {
                return try resp.formatError(self.allocator, "ERR invalid value type");
            },
        };

        try self.storage.set(key, value);
        return try resp.formatSimpleString(self.allocator, "OK");
    }

    fn handleGet(self: *Server, args: []resp.RespValue) ![]u8 {
        if (args.len != 2) {
            return try resp.formatError(self.allocator, "ERR wrong number of arguments for 'get' command");
        }

        const key = switch (args[1]) {
            .bulk_string => |s| s orelse {
                return try resp.formatError(self.allocator, "ERR invalid key");
            },
            else => {
                return try resp.formatError(self.allocator, "ERR invalid key type");
            },
        };

        const value = self.storage.get(key);
        return try resp.formatBulkString(self.allocator, value);
    }

    fn handleDel(self: *Server, args: []resp.RespValue) ![]u8 {
        if (args.len < 2) {
            return try resp.formatError(self.allocator, "ERR wrong number of arguments for 'del' command");
        }

        var deleted_count: i64 = 0;
        for (args[1..]) |arg| {
            const key = switch (arg) {
                .bulk_string => |s| s orelse continue,
                else => continue,
            };

            if (try self.storage.delete(key)) {
                deleted_count += 1;
            }
        }

        return try resp.formatInteger(self.allocator, deleted_count);
    }

    fn handleExists(self: *Server, args: []resp.RespValue) ![]u8 {
        if (args.len < 2) {
            return try resp.formatError(self.allocator, "ERR wrong number of arguments for 'exists' command");
        }

        var exists_count: i64 = 0;
        for (args[1..]) |arg| {
            const key = switch (arg) {
                .bulk_string => |s| s orelse continue,
                else => continue,
            };

            if (self.storage.exists(key)) {
                exists_count += 1;
            }
        }

        return try resp.formatInteger(self.allocator, exists_count);
    }

    fn handleExpire(self: *Server, args: []resp.RespValue) ![]u8 {
        if (args.len != 3) {
            return try resp.formatError(self.allocator, "ERR wrong number of arguments for 'expire' command");
        }

        const key = switch (args[1]) {
            .bulk_string => |s| s orelse {
                return try resp.formatError(self.allocator, "ERR invalid key");
            },
            else => {
                return try resp.formatError(self.allocator, "ERR invalid key type");
            },
        };

        const seconds_str = switch (args[2]) {
            .bulk_string => |s| s orelse {
                return try resp.formatError(self.allocator, "ERR invalid seconds");
            },
            else => {
                return try resp.formatError(self.allocator, "ERR invalid seconds type");
            },
        };

        const seconds = std.fmt.parseInt(i64, seconds_str, 10) catch {
            return try resp.formatError(self.allocator, "ERR value is not an integer or out of range");
        };

        const success = try self.storage.expire(key, seconds);
        return try resp.formatInteger(self.allocator, if (success) 1 else 0);
    }

    fn handleTTL(self: *Server, args: []resp.RespValue) ![]u8 {
        if (args.len != 2) {
            return try resp.formatError(self.allocator, "ERR wrong number of arguments for 'TTL' command");
        }

        const key = switch (args[1]) {
            .bulk_string => |s| s orelse {
                return try resp.formatError(self.allocator, "ERR invalid key");
            },
            else => {
                return try resp.formatError(self.allocator, "ERR invalid key type");
            },
        };

        const ttl = self.storage.ttl(key);

        if (ttl) |t| {
            return try resp.formatInteger(self.allocator, t);
        } else {
            return try resp.formatInteger(self.allocator, -2);
        }
    }

    fn handlePersist(self: *Server, args: []resp.RespValue) ![]u8 {
        if (args.len != 2) {
            return try resp.formatError(self.allocator, "ERR wrong number of arguments for 'persist' command");
        }

        const key = switch (args[1]) {
            .bulk_string => |s| s orelse {
                return try resp.formatError(self.allocator, "Err invalid key");
            },
            else => {
                return try resp.formatError(self.allocator, "ERR invalid key type");
            },
        };

        const success = try self.storage.persist(key);
        return try resp.formatInteger(self.allocator, if (success) 1 else 0);
    }

    fn handleKeys(self: *Server, args: []resp.RespValue) ![]u8 {
        if (args.len != 2) {
            return try resp.formatError(self.allocator, "ERR wrong number of arguments for 'keys' command");
        }

        const pattern = switch (args[1]) {
            .bulk_string => |s| s orelse {
                return try resp.formatError(self.allocator, "ERR invalid pattern");
            },
            else => {
                return try resp.formatError(self.allocator, "ERR invalid pattern type");
            },
        };

        const key_list = try self.storage.keys(self.allocator, pattern);

        defer {
            for (key_list) |key| {
                self.allocator.free(key);
            }
        }

        return try resp.formatArray(self.allocator, key_list);
    }
};
