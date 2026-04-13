import asyncio
import edge_tts

async def generate_voice(text):
    communicate = edge_tts.Communicate(
        text=text,
        voice="ko-KR-SunHiNeural", # 여성
        # voice="ko-KR-InJoonNeural",  # 남성
        rate="-20%" #느리게가 -, 빠르게는 20% 이렇게 설정.
        # pitch="-5Hz" # 톤 조절도 가능한데 선택지가 늘어나는 이슈
    )
    await communicate.save("./images/tts_ver2_f.mp3") # --> 저장경로 및 저장되는 파일이름 즉, images폴더필요.

with open('./example.txt', 'r', encoding='utf-8') as f:
    text = f.read() # 현재는 잘 뽑히나 확인중이라 example.txt라는 메모장으로 사용중임다.

asyncio.run(generate_voice(text))

# 300자 정도까지 안정적이고 500자도 될 수도 있긴하다고 함.
