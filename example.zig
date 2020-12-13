const std = @import("std");

const Condition = @import("./Condition.zig").Condition;
//const ArrayBlockingQueue = @import("./ArrayBlockingQueue.zig").ArrayBlockingQueue;
const LinkedBlockingQueue = @import("./LinkedBlockingQueue.zig").LinkedBlockingQueue;
const CountDownLatch = @import("./CountDownLatch.zig").CountDownLatch;


//const bq = ArrayBlockingQueue(f32);
const bq = LinkedBlockingQueue(f32);

var end: u32 = 0;

const Param = struct {
    queue: *bq,
    id: usize,
    latch: *CountDownLatch,
    rlatch: *CountDownLatch,
};

var result:[16]u32=undefined;

fn example_take(param: Param) !void {
    var queue = param.queue;
    var id = param.id;
    var val:f32=0;
    const inf = std.math.inf(f32);
    param.rlatch.countDown();
    param.latch.wait();

    //std.debug.print("{}  {} start\n", .{id, std.time.nanoTimestamp()});
    var last:f32 =0.0;
    var count:u32 = 0;
    var dst:[2]f32 = undefined;
    var meet_infs:i32 = 0;
    while(meet_infs==0){
        //last = val;
        //val = queue.take();
        const items = queue.takeMany(&dst);
        //if (val == inf)
        
        for(items)|item|{
            if(item == inf)
                meet_infs += 1;
            if(item != inf){
                count += 1;
                last = item;
            }
        }
        
        if(items.len > 500){
            //std.debug.print("batch {}\n", .{items.len});
        }
        
        //count += 1;
        //std.debug.print("{}  {}   take {d:<25}\n", .{id,std.time.nanoTimestamp() , val});
    }
    result[id-8]=count;
    std.debug.print("{}  {}   take end={d}   actual={}   infs={}\n", .{id, std.time.nanoTimestamp(), last, count,meet_infs});
    while(meet_infs > 1){
        queue.put(std.math.inf(f32));
        meet_infs -= 1;
    }
}


fn example_poll(param: Param) !void {
    var queue = param.queue;
    var id = param.id;
    var val:?f32=0;
    const inf = std.math.inf(f32);
    param.rlatch.countDown();
    param.latch.wait();
    var failed_cnt:u32 = 0;

    var last:?f32 =0.0;
    var total_cnt:u32 = 0;
    
    var dst:[20]f32 = undefined;
    var meet_infs:u32 = 0;
    var num: u32 = 0;
    while(meet_infs==0){
        last = val;
        //val = queue.timedPoll(10000);
        val = queue.poll();
        if (val != null and val.? == inf)
            break;
        
        //const items = queue.pollMany(dst[0..]);
        
        //if(items.len > 0){
        //    for(items)|item|{
        //        if(item == inf)
        //            meet_infs += 1;
        //        if(item != inf){
        //            last = item;
        //        }
        //    }
        //} else {
        //    failed_cnt +=1;
        //}
        
        //num += @intCast(u32, items.len);
        
        if(val==null){
            failed_cnt +=1;
        }
        total_cnt += 1;
        //std.debug.print("{}  {}   take {d:<25}\n", .{id,std.time.nanoTimestamp() , val});
    }
    //result[id-8]=num - meet_infs;
    result[id-8]=total_cnt - failed_cnt;
    std.debug.print("{}  {}   poll end={d}   try={}   failed={}  actual={}\n", .{id, std.time.nanoTimestamp(), last, total_cnt,failed_cnt,total_cnt-failed_cnt});
    
    //while(meet_infs > 1){
    //    queue.put(std.math.inf(f32));
    //    meet_infs -= 1;
    //}
}



fn example_put(param: Param) !void {
    var f:i32 = 0;
    var queue = param.queue;
    var id = param.id;
    var succ_cnt:u32 = 0;
    param.rlatch.countDown();
    param.latch.wait();
    //std.debug.print("{}  {} start\n", .{id, std.time.nanoTimestamp()});
    while(end != 0){
        if(f >= 200000){
            break;
        }
        queue.put(@intToFloat(f32, f));
        //std.debug.print("{}  {}   put {d}\n", .{id, std.time.nanoTimestamp() , f});
        f = f + 1;
        succ_cnt += 1;
        //std.time.sleep(700000000);
    }
    queue.put(std.math.inf(f32));
    queue.put(std.math.inf(f32));
    std.debug.print("{}  {}   put end  actual={}\n", .{id, std.time.nanoTimestamp(), succ_cnt});
}

fn example_offer(param: Param) !void {
    var f:i32 = 0;
    var queue = param.queue;
    var id = param.id;
    param.rlatch.countDown();
    param.latch.wait();
    
    var succ_cnt:u32 = 0;
    var total_cnt:u32 = 0;

    while(end != 0){
        if(f >= 200000){
            break;
        }
        //if(queue.offer(@intToFloat(f32, f)) != false){
        if(queue.timedOffer(@intToFloat(f32, f), 10) != false){
            f = f + 1;
            succ_cnt += 1;
        }
        total_cnt += 1;
    }
    queue.put(std.math.inf(f32));
    queue.put(std.math.inf(f32));
    std.debug.print("{}  {}   offer end  try={}   failed={}  actual={}\n", .{id, std.time.nanoTimestamp(), total_cnt,total_cnt-succ_cnt,succ_cnt});
}



pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    var mem:[]u8 = arena.allocator.alloc(u8, 100000000) catch unreachable;
    var tsfba = std.heap.ThreadSafeFixedBufferAllocator.init(mem);

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};



    var queue = bq.init(&gpa.allocator,10000);
    defer queue.deinit();

    var a:[24]*std.Thread = undefined;
    end = 1;

    var latch = CountDownLatch.init(1);
    var rlatch = CountDownLatch.init(24);
    var param = Param{.queue=&queue, .id=0, .latch=&latch , .rlatch=&rlatch};
    for(a[0..4]) |*item|{
        item.* = std.Thread.spawn(param, example_put) catch unreachable;
        param.id+=1;
    }

    for(a[4..8]) |*item|{
        item.* = std.Thread.spawn(param, example_offer) catch unreachable;
        param.id+=1;
    }    

    for(a[8..16]) |*item|{
        item.* = std.Thread.spawn(param, example_take) catch unreachable;
        param.id+=1;
    }
    for(a[16..24]) |*item|{
        item.* = std.Thread.spawn(param, example_poll) catch unreachable;
        param.id+=1;
    }


    rlatch.wait();
    var start = std.time.milliTimestamp();
    latch.countDown();
    for(a)|item, idx|{
        item.wait();
    }
    
    var count:u32 = 0;
    for(result)|v|{
        count += v;
    }
    
    std.debug.print("{}  {}\n", .{std.time.milliTimestamp() - start, count});

}