const std = @import("std");

const Condition = @import("./Condition.zig").Condition;
const CountDownLatch = @import("./CountDownLatch.zig").CountDownLatch;



pub fn LinkedBlockingQueue(comptime T: type) type {
    return struct {
        pub const Node = struct {
            next: ?*Node,
            item: T,
        };
        const Self = @This();

        allocator: ?*std.mem.Allocator,

        capacity: i32 = 0,
        count: i32 = 0,
        
        head: *Node,
        tail: *Node,
        
        putLock: std.Mutex,
        notFull: Condition,

        takeLock: std.Mutex,
        notEmpty: Condition,
        
        
        pub fn init(allocator: *std.mem.Allocator, capacity: anytype) Self {
            var self: Self = undefined;
            self.count = 0;
            self.capacity = @as(i32, capacity);
            self.allocator = allocator;

            var node: *Node = allocator.create(Node) catch unreachable;
            node.next = null;
            node.item = undefined;

            self.head=node;
            self.tail=node;

            self.putLock = std.Mutex{};
            self.takeLock = std.Mutex{};
            self.notEmpty = Condition{.head=null, .tail=null, .mutex=null};
            self.notFull = Condition{.head=null, .tail=null, .mutex=null};
            return self;
        }
        
        pub fn deinit(self: *Self) void {
            var node: ?*Node = self.head;
            while(node != null){
                const next = node.?.next;
                self.allocator.?.destroy(node.?);
                node = next;
            }
            self.head = undefined;
            self.tail = undefined;
            self.allocator = null;
            self.count = 0;
            self.capacity = 0;
        }
        
        pub fn post_init(self: *Self) void {
            if(self.notEmpty.mutex == null)
                self.notEmpty.mutex = &self.takeLock;
            if(self.notFull.mutex == null)
                self.notFull.mutex = &self.putLock;
        }
        
        fn enqueue(self: *Self, node: *Node) void {
            self.tail.next = node;
            self.tail = node;
        }
        
        fn dequeue(self: *Self) *Node {
            const head = self.head;
            const first = head.next;
            if(first == null){
                std.debug.print("maybe you have used an non-thread-safe allocator\n", .{}); 
                unreachable;
            }
            
            self.head = first.?;
            
            head.next = null;
            head.item = first.?.item;
            first.?.item = undefined;
            return head;
        }
        
        fn signalNotEmpty(self: *Self) void {
            const lock = self.takeLock.acquire();
            defer lock.release();
            self.notEmpty.signalAll();
        }
        
        fn signalNotFull(self: *Self) void {
            const lock = self.putLock.acquire();
            defer lock.release();
            self.notFull.signalAll();
        }
        
        fn putNode(self: *Self, node: *Node) void {
            const lock = self.putLock.acquire();
            self.post_init();
            
            while(@atomicLoad(i32, &self.count, .SeqCst) == self.capacity){
                self.notFull.wait();
            }
            
            self.enqueue(node);

            const prev = @atomicRmw(i32, &self.count, .Add, 1, .SeqCst);
            if(prev + 1 < self.capacity){
                self.notFull.signalAll();
            }
            lock.release();
            if(prev == 0){
                self.signalNotEmpty();
            }
        }

        pub fn put(self: *Self, item: T) void {
            var node: *Node = self.allocator.?.create(Node) catch unreachable;
            node.next = null;
            node.item = item;
            
            self.putNode(node);
        }
        
        
        fn timedOfferNode(self: *Self, node: *Node, timeout: u64) bool {
            var lock = self.putLock.acquire();
            self.post_init();
            var t = @intCast(i64, timeout);
            while(@atomicLoad(i32, &self.count, .SeqCst) == self.capacity){
                if(t <= 0) {
                    //std.debug.print("timeout {}\n", .{t});
                    lock.release();
                    return false;
                }
                t = self.notFull.timedWait(@intCast(u64, t));
            }
            
            self.enqueue(node);

            const prev = @atomicRmw(i32, &self.count, .Add, 1, .SeqCst);
            if(prev + 1 < self.capacity){
                self.notFull.signalAll();
            }
            lock.release();
            if(prev == 0){
                self.signalNotEmpty();
            }
            return true;
        }
        
        pub fn timedOffer(self: *Self, item: T, timeout: u64) bool {
            var node: *Node = self.allocator.?.create(Node) catch unreachable;
            node.next = null;
            node.item = item;
            
            return self.timedOfferNode(node, timeout);
        }
        
        
        fn offerNode(self: *Self, node: *Node) bool {
            var lock = self.putLock.acquire();
            self.post_init();
            
            if(@atomicLoad(i32, &self.count, .SeqCst) == self.capacity){
                lock.release();
                return false;
            }
            
            self.enqueue(node);

            const prev = @atomicRmw(i32, &self.count, .Add, 1, .SeqCst);
            if(prev + 1 < self.capacity){
                self.notFull.signalAll();
            }
            lock.release();
            if(prev == 0){
                self.signalNotEmpty();
            }
            return true;
        }
        
        pub fn offer(self: *Self, item: T) bool {
            var node: *Node = self.allocator.?.create(Node) catch unreachable;
            node.next = null;
            node.item = item;
            
            return self.offerNode(node);
        }
        
        pub fn take(self: *Self) T {
            const lock = self.takeLock.acquire();
            self.post_init();
            while(@atomicLoad(i32, &self.count, .SeqCst) == 0){
                self.notEmpty.wait();
            }
            
            const node = self.dequeue();
            const item = node.item;
            const prev = @atomicRmw(i32, &self.count, .Sub, 1, .SeqCst);
            if(prev > 1){
                self.notEmpty.signalAll();
            }
            lock.release();
            if(prev == self.capacity){
                self.signalNotFull();
            }
            self.allocator.?.destroy(node);
            return item;
        }
        
        
        pub fn takeManyO(self: *Self, dst: []T) []T {
            const lock = self.takeLock.acquire();
            self.post_init();
            var count = @atomicLoad(i32, &self.count, .SeqCst);
            while(count == 0){
                self.notEmpty.wait();
                count = @atomicLoad(i32, &self.count, .SeqCst);
            }
            
            const num = std.math.min(dst.len, @intCast(usize, count));
            var idx:usize = 0;
            while(idx < num){
                std.testing.expect(self.head.next != null);
                const node = self.dequeue();
                dst[idx] = node.item;
                self.allocator.?.destroy(node);
                idx += 1;
            }
            
            const prev = @atomicRmw(i32, &self.count, .Sub, @intCast(i32, idx), .SeqCst);
            if(prev > idx){
                self.notEmpty.signalAll();
            }
            lock.release();
            if(prev == self.capacity){
                self.signalNotFull();
            }

            return dst[0..idx];
        }
        
        pub fn takeMany(self: *Self, dst: []T) []T {
            const lock = self.takeLock.acquire();
            self.post_init();
            var count = @atomicLoad(i32, &self.count, .SeqCst);
            while(count == 0){
                self.notEmpty.wait();
                count = @atomicLoad(i32, &self.count, .SeqCst);
            }
            
            const num = std.math.min(dst.len, @intCast(usize, count));
            if(num == 0){
                lock.release();
                return dst[0..0];
            }
            
            var start: *Node = self.head;
            var end: ?*Node = self.head;
            var i: usize = 0;
            while(i<num){
                end = end.?.next;
                i += 1;
            }
            const lastItem = end.?.item;
            end.?.item = undefined;
            self.head = end.?;
            
            const prev = @atomicRmw(i32, &self.count, .Sub, @intCast(i32, num), .SeqCst);
            if(prev > num){
                self.notEmpty.signalAll();
            }
            lock.release();
            
            if(prev == self.capacity){
                self.signalNotFull();
            }

            i = 0;
            var h = start.next.?;
            self.allocator.?.destroy(start);
            start = h;
            while(i<num-1){
                dst[i] = start.item;
                h = start.next.?;
                self.allocator.?.destroy(start);
                start = h;
                i += 1;
            }
            dst[i] = lastItem;
            
            return dst[0..num];
        }
        
        
        pub fn poll(self: *Self) ?T {
            const lock = self.takeLock.acquire();
            self.post_init();
            if(@atomicLoad(i32, &self.count, .SeqCst) == 0){
                lock.release();
                return null;
            }
            
            const node = self.dequeue();
            const item = node.item;
            const prev = @atomicRmw(i32, &self.count, .Sub, 1, .SeqCst);
            if(prev > 1){
                self.notEmpty.signalAll();
            }
            lock.release();
            if(prev == self.capacity){
                self.signalNotFull();
            }
            self.allocator.?.destroy(node);
            return item;
        }

        pub fn peek(self: *Self) ?T {
            const lock = self.takeLock.acquire();
            self.post_init();
            defer lock.release();
            if(@atomicLoad(i32, &self.count, .SeqCst) == 0){
                return null;
            }
            
            const item = self.head.next.?.item;
            return item;
        }

        pub fn timedPoll(self: *Self, timeout: u64) ?T {
            const lock = self.takeLock.acquire();
            self.post_init();
            var t = @intCast(i64, timeout);
            while(@atomicLoad(i32, &self.count, .SeqCst) == 0){
                if(t <= 0) {
                    lock.release();
                    return null;
                }
                t = self.notEmpty.timedWait(@intCast(u64, t));
            }
            
            const node = self.dequeue();
            const item = node.item;
            const prev = @atomicRmw(i32, &self.count, .Sub, 1, .SeqCst);
            if(prev > 1){
                self.notEmpty.signalAll();
            }
            lock.release();
            if(prev == self.capacity){
                self.signalNotFull();
            }
            self.allocator.?.destroy(node);
            return item;
        }
        
    };
}
