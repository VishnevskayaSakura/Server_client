import asyncio

# Command processing function
async def handle_command(writer, cmd_bytes, metrics_data, metrics_lock):
    
    cmd_list = cmd_bytes[:-1].decode('utf-8').split()
    if len(cmd_list) < 2:
        writer.write(b'error\nwrong command\n\n')
        await writer.drain()
        return
    
    if cmd_list[0] == 'get':
        if len(cmd_list) > 2:
            writer.write(b'error\nwrong command\n\n')
            await writer.drain()
            return
        metrics = []
        async with metrics_lock: # we use metrics, only one korutin can have access to metrics
            if cmd_list[1] != '*': 
                if cmd_list[1] not in metrics_data: 
                    writer.write(b'ok\n\n') 
                    await writer.drain()
                    return
                metrics = [(cmd_list[1], value) for value in metrics_data[cmd_list[1]]]
            else: 
                for key in metrics_data: 
                    metrics.extend([(key, val) for val in metrics_data[key]])
        # Critical section ended, access to shared metrics no longer needed
        
        reply_bytes = bytearray(b'ok\n')
        for metric in metrics: 
            key, metric_data = metric
            timestamp, metric_value = metric_data
            reply_bytes.extend('{0} {1} {2}\n'.format(key, str(metric_value), str(timestamp)).encode())
        reply_bytes.extend(b'\n')
        writer.write(reply_bytes)
        await writer.drain()
        return


    elif cmd_list[0] == 'put': 
        if len(cmd_list) != 4:
            writer.write(b'error\nwrong command\n\n')
            await writer.drain()
            return
        try:
            (name, value, timestamp) = [t(s) for t, s in zip((str, float, int), cmd_list[1:])]
        except TypeError: 
            writer.write(b'error\nwrong command\n\n')
            await writer.drain()
            return

        # Critical section, access to shared metrics
        async with metrics_lock:
            if name in metrics_data: # If the metric is already there, we have to rewrite or add the value
                overwrite = False
                for key, (timestamp_s, value_s) in enumerate(metrics_data[name]): 
                    if timestamp == timestamp_s: 
                        metrics_data[name][key] = (timestamp, value) 
                        overwrite = True 
                        break
                if not overwrite: 
                    metrics_data[name].append((timestamp, value))
            else: 
                metrics_data[name] = [(timestamp, value)]
        # End of critical section, access to metrics no longer needed
        
        writer.write(b'ok\n\n')
        await writer.drain()
        return
    else: 
        writer.write(b'error\nwrong command\n\n')
        await writer.drain()
        return

# Called for each new client and serves it
async def handle_connection(reader, writer, timeout, metrics_data, metrics_lock):
    while(True):
        data = await reader.readline()
        if len(data) == 0 or data[-1] != 10:
            writer.close() 
            return
        try:
            # We process the received line with timeout 
            await asyncio.wait_for(
                    handle_command(writer, data, metrics_data, metrics_lock),
                    timeout)
        except asyncio.TimeoutError: 
            writer.write(b'error\ntimeout\n\n')
            await writer.drain()

async def run_server_async(host, port, timeout=None):
    
    metrics_data = {}
    metrics_lock = asyncio.Lock()
    server = await asyncio.start_server(
            lambda r, w: handle_connection(r, w, timeout, metrics_data, metrics_lock),
            host, port)
    await server.wait_closed()

# Required function to start the server
def run_server(host, port, timeout=None):
    loop = asyncio.get_event_loop()
    loop.create_task(run_server_async(host, port, timeout))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    run_server('127.0.0.1', 8888)
