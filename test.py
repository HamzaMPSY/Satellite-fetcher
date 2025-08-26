import re

# Your existing regex for batch progress bars
batch_re = re.compile(
    r"^Concurrent Downloads:\s*"             # Description before colon
    r"(?P<percent>\d+)%\|\s*[^\|]*\|\s*"     # Percent and bar (allowing any content between |)
    r"(?P<done>\d+)/(?P<total>\d+)\s*"       # Done/Total tasks
    r"\[\s*(?P<elapsed>[0-9:?]+)<(?P<eta>[^\]]+)\]\s*"  # Elapsed and ETA/remaining (anything until ])
    r"(?P<rate>[^\s]*/?[^\s]*?)?\s*$"        # Optional rate like '?it/s' or '5.00it/s'
)


# New regex for download progress bars (the ones in your log)
download_re = re.compile(
    r"^Downloading\s+(?P<filename>.+?):\s*"  # "Downloading" + filename
    r"(?P<percent>\d+)%\|\s*.*\|\s*"         # Percent and progress bar
    r"(?P<done>[\d\.]+[kMGTP]?)/(?P<total>\d+)"  # Downloaded/Total (e.g., "44.0k/77")
    r"(?P<remaining>.*?)$"                    # Any remaining text
)

def clean_line(line):
    """Remove ANSI escape codes and other control characters"""
    # Remove ANSI escape sequences
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    line = ansi_escape.sub('', line)
    
    # Remove carriage returns and other control chars
    line = line.replace('\r', '').replace('\x1b', '')
    
    # Remove [A sequences (cursor movement)
    line = re.sub(r'\[A+', '', line)
    
    return line.strip()

def scan_file(path: str):
    results = []
    with open(path, "r", errors="ignore") as f:
        for line_num, line in enumerate(f, 1):
            original_line = line.strip()
            if not original_line:
                continue
            
            # Clean the line of ANSI codes and control characters
            clean_line_text = clean_line(original_line)
            if not clean_line_text:
                continue

            # Try batch progress bars first
            m1 = batch_re.match(clean_line_text)
            if m1:
                results.append(("batch", m1.groupdict(), line_num))
                continue

            # Try download progress bars
            m2 = download_re.match(clean_line_text)
            if m2:
                results.append(("download", m2.groupdict(), line_num))
                continue

    return results

def print_results(matches):
    """Pretty print the results"""
    for kind, data, line_num in matches:
        print(f"Line {line_num}: [{kind.upper()}]")
        if kind == "unmatched_progress":
            print(f"  Raw: {data['raw']}")
        else:
            for key, value in data.items():
                print(f"  {key}: {value}")
        print()

# Example usage
if __name__ == "__main__":
    log_path = "nohup.out"  # <-- replace with your log/output file
    matches = scan_file(log_path)
    
    print(f"Found {len(matches)} progress bar entries:")
    print("-" * 50)
    
    # Summary by type
    type_counts = {}
    for kind, _, _ in matches:
        type_counts[kind] = type_counts.get(kind, 0) + 1
    
    print("Summary:")
    for ptype, count in type_counts.items():
        print(f"  {ptype}: {count} entries")